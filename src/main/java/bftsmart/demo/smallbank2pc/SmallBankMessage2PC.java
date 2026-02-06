package bftsmart.demo.smallbank2pc;

import java.io.*;

/**
 * Message format for SmallBank benchmark transactions
 *
 * @author Prajwal
 */
public class SmallBankMessage2PC implements Serializable {

    private static final long serialVersionUID = 7298765432108765433L;

    public enum TransactionType {
        DEPOSIT_CHECKING,
        TRANSACT_SAVINGS,
        WRITE_CHECK,
        SEND_PAYMENT,
        AMALGAMATE,
        BALANCE,
        CREATE_ACCOUNT,
        RESPONSE,
        ERROR
    }

    /**
     * Two-Phase Commit message types
     */
    public enum TwoPhaseType {
        NONE,           // Not a 2PC message (regular transaction)
        PREPARE,        // Phase 1: Request to prepare/vote
        PREPARE_OK,     // Phase 1 response: Vote YES (can commit)
        PREPARE_FAIL,   // Phase 1 response: Vote NO (must abort)
        COMMIT,         // Phase 2: Commit the transaction
        ABORT,          // Phase 2: Abort the transaction
        ACK,            // Acknowledgment of commit/abort
    }

    private TransactionType txType;
    private long customerId;
    private String customerName;
    private long destCustomerId;  // For SendPayment
    private double amount;
    private double savingsBalance;
    private double checkingBalance;
    private int result;
    private String errorMsg;

    // 2PC fields
    private TwoPhaseType twoPhaseType = TwoPhaseType.NONE;
    private String transactionId;       // Global unique transaction ID
    private int coordinatorShardId;     // Shard that initiated the 2PC
    private int participantShardId;     // Target shard for this message

    private SmallBankMessage2PC() {
        super();
        result = -1;
    }

    private static String generateTransactionId() {
        return String.format("%d-%d", System.currentTimeMillis(),
                System.nanoTime());
    }

    // CreateAccount
    public static SmallBankMessage2PC newCreateAccountRequest(long customerId, String customerName,
                                                           double savingsBalance, double checkingBalance) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.txType = TransactionType.CREATE_ACCOUNT;
        message.customerId = customerId;
        message.customerName = customerName;
        message.savingsBalance = savingsBalance;
        message.checkingBalance = checkingBalance;
        return message;
    }

    // private static String generateTransactionId() {
    //     return String.format("%d-%d", System.currentTimeMillis(),
    //                         txIdCounter.incrementAndGet());
    // }

    // DepositChecking
    public static SmallBankMessage2PC newDepositCheckingRequest(long customerId, double amount, TwoPhaseType twoPhaseType) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.DEPOSIT_CHECKING;
        message.customerId = customerId;
        message.amount = amount;
        if (twoPhaseType != null) {
            message.twoPhaseType = twoPhaseType;
        }
        return message;
    }

    // TransactSavings
    public static SmallBankMessage2PC newTransactSavingsRequest(long customerId, double amount) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.TRANSACT_SAVINGS;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    // WriteCheck
    public static SmallBankMessage2PC newWriteCheckRequest(long customerId, double amount, TwoPhaseType twoPhaseType) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.WRITE_CHECK;
        message.customerId = customerId;
        message.amount = amount;
        if (twoPhaseType != null) {
            message.twoPhaseType = twoPhaseType;
        }
        return message;
    }

    // SendPayment
    public static SmallBankMessage2PC newSendPaymentRequest(long sourceCustomerId, long destCustomerId, double amount) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.SEND_PAYMENT;
        message.customerId = sourceCustomerId;
        message.destCustomerId = destCustomerId;
        message.amount = amount;
        return message;
    }

    // Amalgamate
    public static SmallBankMessage2PC newAmalgamateRequest(long customerId1, long customerId2) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.AMALGAMATE;
        message.customerId = customerId1;
        message.destCustomerId = customerId2;
        return message;
    }

    public static SmallBankMessage2PC newBalanceRequest(long customerId) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.transactionId = generateTransactionId();
        message.txType = TransactionType.BALANCE;
        message.customerId = customerId;
        return message;
    }

    // Response message
    public static SmallBankMessage2PC newResponse(int result) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.txType = TransactionType.RESPONSE;
        message.result = result;
        return message;
    }

    // Response with balance information
    public static SmallBankMessage2PC newResponseWithBalances(int result, double savingsBalance, double checkingBalance) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.txType = TransactionType.RESPONSE;
        message.result = result;
        message.savingsBalance = savingsBalance;
        message.checkingBalance = checkingBalance;
        return message;
    }

    // Error message
    public static SmallBankMessage2PC newErrorMessage(String errorMsg) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.txType = TransactionType.ERROR;
        message.errorMsg = errorMsg;
        return message;
    }

    // 2PC Factory Methods

    /**
     * Create a PREPARE message for 2PC.
     *
     * @param transactionId      Global unique transaction ID
     * @param coordinatorShardId Shard initiating the 2PC
     * @param participantShardId Target shard for this prepare
     * @param innerRequest       The actual transaction to prepare
     * @return PREPARE message
     */
    public static SmallBankMessage2PC newPrepareRequest(String transactionId,
                                                        int coordinatorShardId,
                                                        int participantShardId,
                                                        SmallBankMessage2PC innerRequest) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.PREPARE;
        message.transactionId = transactionId;
        message.coordinatorShardId = coordinatorShardId;
        message.participantShardId = participantShardId;
        // Copy inner request fields
        message.txType = innerRequest.txType;
        message.customerId = innerRequest.customerId;
        message.customerName = innerRequest.customerName;
        message.destCustomerId = innerRequest.destCustomerId;
        message.amount = innerRequest.amount;
        message.savingsBalance = innerRequest.savingsBalance;
        message.checkingBalance = innerRequest.checkingBalance;
        return message;
    }

    /**
     * Create a simple PREPARE message for single-account operations.
     */
    public static SmallBankMessage2PC newPrepareRequest(String transactionId,
                                                        int coordinatorShardId,
                                                        int participantShardId,
                                                        TransactionType txType,
                                                        long customerId,
                                                        double amount) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.PREPARE;
        message.transactionId = transactionId;
        message.coordinatorShardId = coordinatorShardId;
        message.participantShardId = participantShardId;
        message.txType = txType;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    /**
     * Create a PREPARE_OK response (vote YES).
     */
    public static SmallBankMessage2PC newPrepareOk(String transactionId) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.PREPARE_OK;
        message.transactionId = transactionId;
        message.result = 0;
        return message;
    }

    /**
     * Create a PREPARE_FAIL response (vote NO).
     */
    public static SmallBankMessage2PC newPrepareFail(String transactionId, String reason) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.PREPARE_FAIL;
        message.transactionId = transactionId;
        message.errorMsg = reason;
        message.result = -1;
        return message;
    }

    /**
     * Create a COMMIT message for phase 2.
     */
    public static SmallBankMessage2PC newCommit(String transactionId, int coordinatorShardId) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.COMMIT;
        message.transactionId = transactionId;
        message.coordinatorShardId = coordinatorShardId;
        return message;
    }

    /**
     * Create a COMMIT message with transaction details for phase 2.
     * The server needs these details to execute the actual transaction.
     *
     * @param transactionId      The transaction ID
     * @param coordinatorShardId The coordinator shard ID
     * @param txType             The transaction type to execute
     * @param customerId         The customer ID for this operation
     * @param amount             The amount for this operation
     * @return COMMIT message with transaction details
     */
    public static SmallBankMessage2PC newCommitWithDetails(String transactionId, int coordinatorShardId,
                                                            TransactionType txType, long customerId, double amount) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.COMMIT;
        message.transactionId = transactionId;
        message.coordinatorShardId = coordinatorShardId;
        message.txType = txType;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    /**
     * Create an ABORT message for phase 2.
     */
    public static SmallBankMessage2PC newAbort(String transactionId, int coordinatorShardId) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.ABORT;
        message.transactionId = transactionId;
        message.coordinatorShardId = coordinatorShardId;
        return message;
    }

    /**
     * Create an ACK response for commit/abort.
     */
    public static SmallBankMessage2PC newAck(String transactionId) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        message.twoPhaseType = TwoPhaseType.ACK;
        message.transactionId = transactionId;
        message.result = 0;
        return message;
    }

    /**
     * Create a CROSS_SHARD_REQUEST message.
     * Client sends this to the coordinator shard (shard containing sender account).
     * The leader of that shard will coordinate the 2PC.
     *
     * @param txType         The transaction type (SEND_PAYMENT or AMALGAMATE)
     * @param customerId     The source/first customer ID
     * @param destCustomerId The destination/second customer ID
     * @param amount         The transaction amount
     * @return CROSS_SHARD_REQUEST message
     */
    public static SmallBankMessage2PC newCrossShardRequest(
            TransactionType txType, long customerId, long destCustomerId, double amount) {
        SmallBankMessage2PC message = new SmallBankMessage2PC();
        // message.twoPhaseType = TwoPhaseType.CROSS_SHARD_REQUEST;
        message.txType = txType;
        message.customerId = customerId;
        message.destCustomerId = destCustomerId;
        message.amount = amount;
        return message;
    }

    // 2PC Helper Methods
    public boolean is2PCMessage() {
        return twoPhaseType != TwoPhaseType.NONE;
    }

    public boolean isPrepareOk() {
        return twoPhaseType == TwoPhaseType.PREPARE_OK;
    }

    public boolean isPrepareFail() {
        return twoPhaseType == TwoPhaseType.PREPARE_FAIL;
    }

    // Serialize to bytes
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            oos.flush();
            baos.flush();
            byte[] bytes = baos.toByteArray();
            oos.close();
            baos.close();
            return bytes;
        } catch (IOException ex) {
            return null;
        }
    }

    // Deserialize from bytes
    public static SmallBankMessage2PC getObject(byte[] bytes) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            SmallBankMessage2PC message = (SmallBankMessage2PC) ois.readObject();
            ois.close();
            bais.close();
            return message;
        } catch (ClassNotFoundException | IOException ex) {
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SmallBankMessage2PC(");
        if (twoPhaseType != TwoPhaseType.NONE) {
            sb.append("2PC=").append(twoPhaseType);
            sb.append(", txId=").append(transactionId);
            sb.append(", coordinator=").append(coordinatorShardId);
            sb.append(", ");
        }
        sb.append("type=").append(txType);
        sb.append(", customerId=").append(customerId);
        if (destCustomerId > 0) {
            sb.append(", destCustomerId=").append(destCustomerId);
        }
        if (amount != 0) {
            sb.append(", amount=").append(amount);
        }
        if (customerName != null) {
            sb.append(", customerName=").append(customerName);
        }
        if (savingsBalance != 0) {
            sb.append(", savingsBalance=").append(savingsBalance);
        }
        if (checkingBalance != 0) {
            sb.append(", checkingBalance=").append(checkingBalance);
        }
        sb.append(", result=").append(result);
        if (errorMsg != null) {
            sb.append(", error=").append(errorMsg);
        }
        sb.append(")");
        return sb.toString();
    }

    public TransactionType getTxType() {
        return txType;
    }

    public long getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public long getDestCustomerId() {
        return destCustomerId;
    }

    public double getAmount() {
        return amount;
    }

    public double getSavingsBalance() {
        return savingsBalance;
    }

    public double getCheckingBalance() {
        return checkingBalance;
    }

    public int getResult() {
        return result;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public TwoPhaseType getTwoPhaseType() {
        return twoPhaseType;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public int getCoordinatorShardId() {
        return coordinatorShardId;
    }

    public int getParticipantShardId() {
        return participantShardId;
    }
}
