/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.statemanagement.standard;

import bftsmart.statemanagement.StateManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.tom.leaderchange.CertifiedDecision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Marcel Santos
 *
 */
public class StandardStateManager extends StateManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int replica;
    private ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;
    private Timer periodicStateRequestTimer = null;
    private final static long INIT_TIMEOUT = 40000;
    private final static long MAX_TIMEOUT = 300000; // 5 minutes cap for state-transfer retry backoff
    private final static long PERIODIC_STATE_REQUEST_RETRY_MS = 2500;
    private long timeout = INIT_TIMEOUT;

    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {

        super.init(tomLayer, dt);

        changeReplica(); // initialize replica from which to ask the complete state

    }

    private void changeReplica() {

        int[] processes = this.SVController.getCurrentViewOtherAcceptors();
        Random r = new Random();

        int pos;
        do {
            //pos = this.SVController.getCurrentViewPos(replica);
            //replica = this.SVController.getCurrentViewProcesses()[(pos + 1) % SVController.getCurrentViewN()];

            if (processes != null && processes.length > 1) {
                pos = r.nextInt(processes.length);
                replica = processes[pos];
            } else {
                replica = 0;
                break;
            }
        } while (replica == SVController.getStaticConf().getProcessId());
    }

    @Override
    protected void requestState() {
        if (tomLayer.requestsTimer != null) {
            tomLayer.requestsTimer.clearAll();
        }

        changeReplica(); // always ask the complete state to a different replica

        sendStateRequest(waitingCID, replica);
        restartPeriodicStateRequestRetry();

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                StandardSMMessage msg = new StandardSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, -1, null, null, -1, -1);
                triggerTimeout(msg);
            }
        };

        stateTimer = new Timer("state timer");
        if (timeout > (MAX_TIMEOUT / 2)) {
            timeout = MAX_TIMEOUT;
        } else {
            timeout = timeout * 2;
        }
        stateTimer.schedule(stateTask, timeout);
    }

    private void sendStateRequest(int cid, int expectedReplica) {
        SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                cid, TOMUtil.SM_REQUEST, expectedReplica, null, null, -1, -1);
        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), smsg);
        logger.info("I just sent a request to the other replicas for the state up to CID " + cid
                + " (expectedReplica=" + expectedReplica + ")");
    }

    private void restartPeriodicStateRequestRetry() {
        cancelPeriodicStateRequestRetry();
        periodicStateRequestTimer = new Timer("state periodic retry timer");
        periodicStateRequestTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                lockTimer.lock();
                try {
                    if (waitingCID == -1 || !SVController.getStaticConf().isStateTransferEnabled()) {
                        cancelPeriodicStateRequestRetry();
                        return;
                    }

                    logger.info("Periodic state-transfer retry for CID {} (expectedReplica={})", waitingCID, replica);
                    sendStateRequest(waitingCID, replica);
                } finally {
                    lockTimer.unlock();
                }
            }
        }, PERIODIC_STATE_REQUEST_RETRY_MS, PERIODIC_STATE_REQUEST_RETRY_MS);
    }

    private void cancelPeriodicStateRequestRetry() {
        if (periodicStateRequestTimer != null) {
            periodicStateRequestTimer.cancel();
            periodicStateRequestTimer = null;
        }
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        logger.debug("Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
        if (stateTimer != null) {
            stateTimer.cancel();
        }
        changeReplica();
        reset();
        requestState();
        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (SVController.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
            StandardSMMessage stdMsg = (StandardSMMessage) msg;
            boolean sendState = stdMsg.getReplica() == SVController.getStaticConf().getProcessId();
            logger.info("Received SM_REQUEST from replica {} for CID {} (requestedReplica={}, localReplica={}, sendState={})",
                    msg.getSender(),
                    msg.getCID(),
                    stdMsg.getReplica(),
                    SVController.getStaticConf().getProcessId(),
                    sendState);

            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);
            if (thisState == null) {

                logger.warn("For some reason, I am sending a void state");
                thisState = dt.getRecoverer().getState(-1, sendState);
            }
            int serializedBytes = (thisState != null && thisState.getSerializedState() != null)
                    ? thisState.getSerializedState().length : -1;
            logger.info("Preparing SM_REPLY to replica {} for CID {} (hasState={}, serializedBytes={}, lastCID={})",
                    msg.getSender(),
                    msg.getCID(),
                    (thisState != null && thisState.hasState()),
                    serializedBytes,
                    (thisState != null ? thisState.getLastCID() : -1));

            int[] targets = {msg.getSender()};
            SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    msg.getCID(), TOMUtil.SM_REPLY, -1, thisState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());

            logger.info("Sending state reply to replica {} for CID {}", msg.getSender(), msg.getCID());
            tomLayer.getCommunication().send(targets, smsg);
            logger.info("State reply sent to replica {} for CID {}", msg.getSender(), msg.getCID());
        }
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        lockTimer.lock();
        try {
            if (SVController.getStaticConf().isStateTransferEnabled()) {
                ApplicationState replyState = msg.getState();
                int serializedBytes = (replyState != null && replyState.getSerializedState() != null)
                        ? replyState.getSerializedState().length : -1;
                logger.info(
                        "Received SM_REPLY from replica {} for CID {} (waitingCID={}, targetCID={}, appStateOnly={}, expectedReplica={}, hasState={}, serializedBytes={})",
                        msg.getSender(),
                        msg.getCID(),
                        waitingCID,
                        targetCID,
                        appStateOnly,
                        replica,
                        (replyState != null && replyState.hasState()),
                        serializedBytes);

                if (replyState == null) {
                    logger.warn("Ignoring SM_REPLY from replica {} for CID {} because payload state is null",
                            msg.getSender(),
                            msg.getCID());
                    return;
                }

                if (waitingCID == -1 || msg.getCID() != waitingCID) {
                    logger.info("Ignoring SM_REPLY from replica {} for CID {} because waitingCID is {}",
                            msg.getSender(),
                            msg.getCID(),
                            waitingCID);
                    return;
                }

                int currentRegency = -1;
                int currentLeader = -1;
                View currentView = null;
                CertifiedDecision currentProof = null;

                if (!appStateOnly) {
                    senderRegencies.put(msg.getSender(), msg.getRegency());
                    senderLeaders.put(msg.getSender(), msg.getLeader());
                    senderViews.put(msg.getSender(), msg.getView());
                    senderProofs.put(msg.getSender(), replyState.getCertifiedDecision(SVController));
                    if (enoughRegencies(msg.getRegency())) {
                        currentRegency = msg.getRegency();
                    }
                    if (enoughLeaders(msg.getLeader())) {
                        currentLeader = msg.getLeader();
                    }
                    if (enoughViews(msg.getView())) {
                        currentView = msg.getView();
                    }
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager())) {
                        currentProof = replyState.getCertifiedDecision(SVController);
                    }

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                if (msg.getSender() == replica && replyState.getSerializedState() != null) {
                    logger.debug("Expected replica sent state. Setting it to state");
                    state = replyState;
                    if (stateTimer != null) {
                        stateTimer.cancel();
                    }
                } else if (msg.getSender() == replica) {
                    logger.info("Expected replica {} replied for CID {} but serialized state is null",
                            replica,
                            waitingCID);
                }

                senderStates.put(msg.getSender(), replyState);
                logger.info("Accepted SM_REPLY from replica {} for CID {} (collectedReplies={}, required>{})",
                        msg.getSender(),
                        waitingCID,
                        getReplies(),
                        SVController.getCurrentViewF());

                logger.debug("Verifying more than F replies");
                if (enoughReplies()) {
                    logger.debug("More than F confirmed");
                    ApplicationState otherReplicaState = getOtherReplicaState();
                    int haveState = 0;
                    if (state != null) {
                        byte[] hash = null;
                        hash = tomLayer.computeHash(state.getSerializedState());
                        if (otherReplicaState != null) {
                            if (Arrays.equals(hash, otherReplicaState.getStateHash())) {
                                haveState = 1;
                            } else if (getNumEqualStates() > SVController.getCurrentViewF()) {
                                haveState = -1;
                            }
                        }
                        TreeMap<Integer, TOMMessage> lastReplies = ((DefaultApplicationState) state).getLastReplies();
                        logger.debug("DefaultApplicationState lastReplies TreeMap :: size=" + lastReplies.size());
                    }
                    logger.info(
                            "State transfer gates for CID {}: haveState={}, otherReplicaStatePresent={}, currentRegency={}, currentLeader={}, currentViewPresent={}, proofPresent={}, appStateOnly={}, replies={}",
                            waitingCID,
                            haveState,
                            (otherReplicaState != null),
                            currentRegency,
                            currentLeader,
                            (currentView != null),
                            (currentProof != null),
                            appStateOnly,
                            getReplies());

                    if (otherReplicaState != null && haveState == 1 && currentRegency > -1
                            && currentLeader > -1 && currentView != null && (!isBFT || currentProof != null || appStateOnly)) {

                        logger.info("Received state. Will install it (CID {}, expectedReplica {}, stateLastCID {})",
                                waitingCID,
                                replica,
                                state.getLastCID());

                        tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);
                        tomLayer.execManager.setNewLeader(currentLeader);

                        if (currentProof != null && !appStateOnly) {

                            logger.debug("Installing proof for consensus " + waitingCID);

                            Consensus cons = execManager.getConsensus(waitingCID);
                            Epoch e = null;

                            for (ConsensusMessage cm : currentProof.getConsMessages()) {

                                e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                if (e.getTimestamp() != cm.getEpoch()) {

                                    logger.warn("Strange... proof contains messages from more than just one epoch");
                                    e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                }
                                e.addToProof(cm);

                                if (cm.getType() == MessageFactory.ACCEPT) {
                                    e.setAccept(cm.getSender(), cm.getValue());
                                } else if (cm.getType() == MessageFactory.WRITE) {
                                    e.setWrite(cm.getSender(), cm.getValue());
                                }

                            }

                            if (e != null) {

                                byte[] hash = tomLayer.computeHash(currentProof.getDecision());
                                e.propValueHash = hash;
                                e.propValue = currentProof.getDecision();
                                e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(), false);
                                cons.decided(e, false);

                                logger.info("Successfully installed proof for consensus " + waitingCID);

                            } else {
                                logger.error("Failed to install proof for consensus " + waitingCID);

                            }

                        }

                        // I might have timed out before invoking the state transfer, so
                        // stop my re-transmission of STOP messages for all regencies up to the current one
                        if (currentRegency > 0) {
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        }
                        //if (currentRegency > 0)
                        //    tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() * (currentRegency * 2));

                        logger.info("Pausing decision delivery before applying transferred state for CID {}", waitingCID);
                        dt.pauseDecisionDelivery();
                        cancelPeriodicStateRequestRetry();
                        waitingCID = -1;
                        targetCID = -1;
                        logger.info("Invoking DeliveryThread.update with transferred state (lastCID={})", state.getLastCID());
                        dt.update(state);
                        logger.info("DeliveryThread.update completed (lastCID={})", state.getLastCID());

                        if (!appStateOnly && execManager.stopped()) {
                            logger.info("Execution manager is stopped; reinserting stopped messages after state install");
                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
                            for (ConsensusMessage stopped : stoppedMsgs) {
                                if (stopped.getNumber() > state.getLastCID() /*msg.getCID()*/) {
                                    execManager.addOutOfContextMessage(stopped);
                                }
                            }
                            execManager.clearStopped();
                            execManager.restart();
                        }

                        logger.info("Processing out-of-context messages after state install");
                        tomLayer.processOutOfContext();

                        if (SVController.getCurrentViewId() != currentView.getId()) {
                            logger.info("Installing current view!");
                            SVController.reconfigureTo(currentView);
                        }

                        isInitializing = false;

                        logger.info("Resuming decision delivery after state install");
                        dt.canDeliver();
                        dt.resumeDecisionDelivery();

                        reset();

                        logger.info("I updated the state!");

                        if (tomLayer.requestsTimer != null) {
                            int refreshedRequests = tomLayer.requestsTimer.refreshWatchedTimeoutsAfterStateTransfer();
                            logger.info("Refreshed timeout tracking for {} watched requests after state transfer",
                                    refreshedRequests);
                            tomLayer.requestsTimer.Enabled(true);
                            tomLayer.requestsTimer.startTimer();
                        }
                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            appStateOnly = false;
                            logger.info("Resuming leader-change protocol after app-state-only transfer");
                            tomLayer.getSynchronizer().resumeLC();
                        }
                    } else if (otherReplicaState == null && (SVController.getCurrentViewN() / 2) < getReplies()) {
                        int retryCID = (targetCID >= 0 ? targetCID : waitingCID);
                        logger.info(
                                "State transfer branch (CID {}): missing comparison state despite replies={} (> N/2={}), resetting and{}",
                                waitingCID,
                                getReplies(),
                                (SVController.getCurrentViewN() / 2),
                                (appStateOnly ? " re-requesting app state" : " waiting for next trigger"));
                        cancelPeriodicStateRequestRetry();
                        waitingCID = -1;
                        if (!appStateOnly) {
                            targetCID = -1;
                        }
                        reset();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            waitingCID = retryCID;
                            logger.info("Retrying app-state transfer after missing comparison state for CID {}", waitingCID);
                            requestState();
                        }
                    } else if (haveState == -1) {
                        logger.info(
                                "State transfer branch (CID {}): expected replica {} state mismatched quorum hash or was missing; rotating expected replica and retrying",
                                waitingCID,
                                replica);

                        changeReplica();
                        reset();
                        requestState();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                    } else if (haveState == 0 && (SVController.getCurrentViewN() - SVController.getCurrentViewF()) <= getReplies()) {

                        logger.info(
                                "State transfer branch (CID {}): could not obtain installable state with replies={} (needed>={}); clearing waitingCID",
                                waitingCID,
                                getReplies(),
                                (SVController.getCurrentViewN() - SVController.getCurrentViewF()));
                        reset();
                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                        cancelPeriodicStateRequestRetry();
                        waitingCID = -1;
                        targetCID = -1;
                        //requestState();
                    } else {
                        logger.info("State transfer not yet finished for CID {} (haveState={}, otherReplicaStatePresent={}, replies={}, required>{})",
                                waitingCID,
                                haveState,
                                (otherReplicaState != null),
                                getReplies(),
                                SVController.getCurrentViewF());

                    }
                } else {
                    logger.info("State transfer waiting for more replies for CID {} (replies={}, required>{})",
                            waitingCID,
                            getReplies(),
                            SVController.getCurrentViewF());
                }
            }
        } finally {
            lockTimer.unlock();
        }
    }

    /**
     * Search in the received states table for a state that was not sent by the
     * expected replica. This is used to compare both states after received the
     * state from expected and other replicas.
     *
     * @return The state sent from other replica
     */
    private ApplicationState getOtherReplicaState() {
        int[] processes = SVController.getCurrentViewProcesses();
        for (int process : processes) {
            if (process == replica) {
                continue;
            } else {
                ApplicationState otherState = senderStates.get(process);
                if (otherState != null) {
                    return otherState;
                }
            }
        }
        return null;
    }

    private int getNumEqualStates() {
        List<ApplicationState> states = new ArrayList<ApplicationState>(receivedStates());
        int match = 0;
        for (ApplicationState st1 : states) {
            int count = 0;
            for (ApplicationState st2 : states) {
                if (st1 != null && st1.equals(st2)) {
                    count++;
                }
            }
            if (count > match) {
                match = count;
            }
        }
        return match;
    }

}
