package bftsmart.demo.smallbank;

import bftsmart.rlrpc.Prediction;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.Storage;

import java.io.*;
import java.util.HashMap;

public class SmallBankServer extends DefaultRecoverable {
    private static final boolean _debug = false;
    private HashMap<Long, String> accounts;
    private HashMap<Long, Double> checking;
    private HashMap<Long, Double> savings;

    private boolean logPrinted = false;

    /* Adaptive timers */
    private Storage consensusLatency;
    private final int interval;
    private int iterations = 0;
    private ServiceReplica replica;

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            new SmallBankServer(Integer.parseInt(args[0]));
        } else {
            System.out.println("Usage: java ... SmallBankServer <replica_id>");
        }
    }

    private SmallBankServer(int id) {
        this.accounts = new HashMap<>();
        this.checking = new HashMap<>();
        this.savings = new HashMap<>();
        this.interval = 10;
        this.consensusLatency = new Storage(this.interval);
        replica = new ServiceReplica(id, this, this);
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtx, boolean fromConsensus) {
        byte[][] replies = new byte[commands.length][];
        int index = 0;
        for (byte[] command : commands) {
            if (msgCtx != null && msgCtx[index] != null && msgCtx[index].getConsensusId() % 1000 == 0 && !logPrinted) {
                System.out.println("SmallBankServer executing CID: " + msgCtx[index].getConsensusId());
                logPrinted = true;
            } else {
                logPrinted = false;
            }

            /* Adaptive Timers */
            iterations++;
            if (msgCtx != null && msgCtx[index].getFirstInBatch() != null) {
                consensusLatency.store(msgCtx[index].getFirstInBatch().decisionTime - msgCtx[index].getFirstInBatch().consensusStartTime);
            }

            if (iterations % interval == 0 && replica.getLearningAgentClient() != null) {
                try {
                    Prediction prediction = this.replica.getLearningAgentClient()
                            .predict(
                                    interval,
                                    (float) consensusLatency.getAverage(false) / 1000000,
                                    (float) consensusLatency.getMax(false) / 1000000,
                                    (float) consensusLatency.getMin(false) / 1000000,
                                    (float) consensusLatency.getDP(true) / 1000000
                            );
                    System.out.println("Prediction ID: " + prediction.getPredictionId());
                    System.out.println("Suggested timeout: " +
                            prediction.getAction().getTimeoutMilliseconds() + " ms");
                    replica.getRequestsTimer().setShortTimeout(prediction.getAction().getTimeoutMilliseconds());
                    consensusLatency.reset();
                } catch (Exception e) {
                    System.out.println("Exception in getting timeout from agent: " + e.getMessage());
                }
            }

            SmallBankMessage request = SmallBankMessage.getObject(command);
            SmallBankMessage reply = SmallBankMessage.newErrorMessage("Unknown error");

            if (request == null) {
                replies[index] = reply.getBytes();
                continue;
            }

            if (_debug) {
                System.out.println("[INFO] Processing ordered request: " + request.getTxType());
            }

            try {
                switch (request.getTxType()) {
                    case CREATE_ACCOUNT: {
//                        System.out.println("[INFO] Creating account for " + request);
                        long custId = request.getCustomerId();
                        if (accounts.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage("Account already exists");
                        } else {
                            accounts.put(custId, request.getCustomerName());
                            checking.put(custId, request.getCheckingBalance());
                            savings.put(custId, request.getSavingsBalance());
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    case DEPOSIT_CHECKING: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage("Account not found");
                        } else {
                            double balance = checking.get(custId) + request.getAmount();
                            checking.put(custId, balance);
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    case TRANSACT_SAVINGS: {
                        long custId = request.getCustomerId();
                        if (!savings.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage("Account not found");
                        } else {
                            double balance = savings.get(custId) + request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage.newErrorMessage("Insufficient funds");
                            } else {
                                savings.put(custId, balance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case WRITE_CHECK: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage("Account not found");
                        } else {
                            double balance = checking.get(custId) - request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage.newErrorMessage("Insufficient funds");
                            } else {
                                checking.put(custId, balance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case SEND_PAYMENT: {
                        long srcId = request.getCustomerId();
                        long destId = request.getDestCustomerId();

                        if (!checking.containsKey(srcId)) {
                            reply = SmallBankMessage.newErrorMessage("Source account not found");
                        } else if (!checking.containsKey(destId)) {
                            reply = SmallBankMessage.newErrorMessage("Destination account not found");
                        } else {
                            double srcBalance = checking.get(srcId) - request.getAmount();
                            if (srcBalance < 0) {
                                reply = SmallBankMessage.newErrorMessage("Insufficient funds");
                            } else {
                                double destBalance = checking.get(destId) + request.getAmount();
                                checking.put(srcId, srcBalance);
                                checking.put(destId, destBalance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case AMALGAMATE: {
                        long custId1 = request.getCustomerId();
                        long custId2 = request.getDestCustomerId();

                        if (!checking.containsKey(custId1) || !savings.containsKey(custId1)) {
                            reply = SmallBankMessage.newErrorMessage("Account 1 not found");
                        } else if (!checking.containsKey(custId2) || !savings.containsKey(custId2)) {
                            reply = SmallBankMessage.newErrorMessage("Account 2 not found");
                        } else {
                            // Transfer all from custId2's checking to custId1's savings
                            double amountToTransfer = checking.get(custId2);
                            checking.put(custId2, 0.0);
                            savings.put(custId1, savings.get(custId1) + amountToTransfer);
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    default:
                        reply = SmallBankMessage.newErrorMessage("Unknown operation type");
                        break;
                }
            } catch (Exception e) {
                reply = SmallBankMessage.newErrorMessage("Exception: " + e.getMessage());
                if (_debug) {
                    e.printStackTrace();
                }
            }

            if (_debug) {
                System.out.println("[INFO] Sending reply");
            }
            replies[index++] = reply.getBytes();
        }
        return replies;
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        SmallBankMessage request = SmallBankMessage.getObject(command);
        SmallBankMessage reply = SmallBankMessage.newErrorMessage("Unknown error");

        if (request == null) {
            return reply.getBytes();
        }

        switch (request.getTxType()) {
            case BALANCE:
                long custId = request.getCustomerId();
                if (!checking.containsKey(custId) || !savings.containsKey(custId)) {
                    reply = SmallBankMessage.newErrorMessage("Account not found");
                } else {
                    double checkingBalance = checking.get(custId);
                    double savingsBalance = savings.get(custId);
                    reply = SmallBankMessage.newResponseWithBalances(0, checkingBalance, savingsBalance);
                }
                return reply.getBytes();
        }

        return reply.getBytes();
    }

    @Override
    public void installSnapshot(byte[] state) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(state);
            ObjectInput in = new ObjectInputStream(bis);
            accounts = (HashMap<Long, String>) in.readObject();
            checking = (HashMap<Long, Double>) in.readObject();
            savings = (HashMap<Long, Double>) in.readObject();
            in.close();
            bis.close();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR] Error deserializing state: "
                    + e.getMessage());
        }
    }

    @Override
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(accounts);
            out.writeObject(checking);
            out.writeObject(savings);
            out.flush();
            bos.flush();
            out.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error serializing state: "
                    + ioe.getMessage());
            return "ERROR".getBytes();
        }
    }
}
