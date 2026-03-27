/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.tom.leaderchange;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.Set;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.TOMUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class RequestsTimer {
    private static final long MAX_EFFECTIVE_TIMEOUT_MS = 10_000L;

    private enum BackoffIncrementMode {
        LINEAR,
        EXPONENTIAL
    }

    private enum BackoffDecayMode {
        LINEAR,
        EXPONENTIAL,
        RESET
    }

    private enum BackoffDecayTiming {
        CURRENT_VIEW,
        NEXT_VIEW
    }
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Timer timer = new Timer("request timer");
    private RequestTimerTask rtTask = null;
    private TOMLayer tomLayer; // TOM layer
    private long timeout;
    private long shortTimeout;
    private long backoffMultiplier;
    private boolean timeoutBackoffEnabled;
    private BackoffIncrementMode backoffIncrementMode;
    private BackoffDecayMode backoffDecayMode;
    private BackoffDecayTiming backoffDecayTiming;
    private int decayAfterSuccessfulSequences;
    private long successfulSequencesSinceDecay;
    private long pendingDecaySteps;
    private boolean resetBackoffOnNextViewChange;
    private final Object backoffLock = new Object();
    private TreeSet<TOMMessage> watched = new TreeSet<TOMMessage>();
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    private boolean enabled = true;
    
    private ServerCommunicationSystem communication; // Communication system between replicas
    private ServerViewController controller; // Reconfiguration manager
    
    private HashMap <Integer, Timer> stopTimers = new HashMap<>();
    private int highestStopRegencySeen = -1;
    
    //private Storage st1 = new Storage(100000);
    //private Storage st2 = new Storage(10000);
    /**
     * Creates a new instance of RequestsTimer
     * @param tomLayer TOM layer
     */
    public RequestsTimer(TOMLayer tomLayer, ServerCommunicationSystem communication, ServerViewController controller) {
        this.tomLayer = tomLayer;
        
        this.communication = communication;
        this.controller = controller;
        
        this.timeout = this.controller.getStaticConf().getRequestTimeout();
        this.backoffMultiplier = 1;
        this.timeoutBackoffEnabled = this.controller.getStaticConf().isRequestTimeoutBackoffEnabled();
        this.backoffIncrementMode = parseBackoffIncrementMode(this.controller.getStaticConf().getRequestTimeoutBackoffIncrementMode());
        this.backoffDecayMode = parseBackoffDecayMode(this.controller.getStaticConf().getRequestTimeoutBackoffDecayMode());
        this.backoffDecayTiming = parseBackoffDecayTiming(this.controller.getStaticConf().getRequestTimeoutBackoffDecayTiming());
        this.decayAfterSuccessfulSequences = this.controller.getStaticConf().getRequestTimeoutBackoffDecayAfterSuccessfulSequences();
        this.successfulSequencesSinceDecay = 0;
        this.pendingDecaySteps = 0;
        this.resetBackoffOnNextViewChange = true;
        this.shortTimeout = -1;
    }

    public void setShortTimeout(long shortTimeout) {
        synchronized (backoffLock) {
            this.shortTimeout = shortTimeout;
            backoffMultiplier = clampMultiplierToCapLocked(backoffMultiplier);
        }
    }

    public void setShortTimeoutPreservingEffectiveTimeout(long shortTimeout) {
        synchronized (backoffLock) {
            long previousEffectiveTimeout = getTimeoutLocked();
            this.shortTimeout = shortTimeout;

            if (shortTimeout <= 0) {
                backoffMultiplier = clampMultiplierToCapLocked(backoffMultiplier);
                return;
            }

            long adjustedMultiplier = Math.round((double) previousEffectiveTimeout / (double) shortTimeout);
            backoffMultiplier = clampMultiplierToCapLocked(adjustedMultiplier);
        }
    }

    public long getTimeout() {
        synchronized (backoffLock) {
            return getTimeoutLocked();
        }
    }

    private long getTimeoutLocked() {
        long base = getBaseTimeoutLocked();
        backoffMultiplier = clampMultiplierToCapLocked(backoffMultiplier);
        if (backoffMultiplier <= 1) {
            return base;
        }

        long maxMultiplier = (base == 0 ? Long.MAX_VALUE : Long.MAX_VALUE / base);
        long effectiveMultiplier = Math.min(backoffMultiplier, maxMultiplier);
        return base * effectiveMultiplier;
    }
    
    public void startTimer() {
        if (rtTask == null) {
            long t = getTimeout();
            //shortTimeout = -1;
            rtTask = new RequestTimerTask();
            if (controller.getCurrentViewN() > 1) timer.schedule(rtTask, t);
        }
    }
    
    public void stopTimer() {
        if (rtTask != null) {
            rtTask.cancel();
            rtTask = null;
        }
    }
    
    public void Enabled(boolean phase) {
        
        enabled = phase;
    }
    
    public boolean isEnabled() {
    	return enabled;
    }
    
    /**
     * Creates a timer for the given request
     * @param request Request to which the timer is being createf for
     */
    public void watch(TOMMessage request) {
        //long startInstant = System.nanoTime();
        rwLock.writeLock().lock();
        watched.add(request);
        if (watched.size() >= 1 && enabled) startTimer();
        rwLock.writeLock().unlock();
    }

    /**
     * Cancels a timer for a given request
     * @param request Request whose timer is to be canceled
     */
    public void unwatch(TOMMessage request) {
        //long startInstant = System.nanoTime();
        rwLock.writeLock().lock();
        if (watched.remove(request) && watched.isEmpty()) stopTimer();
        rwLock.writeLock().unlock();
    }

    /**
     * Cancels all timers for all messages
     */
    public void clearAll() {
        TOMMessage[] requests = new TOMMessage[watched.size()];
        rwLock.writeLock().lock();
        
        watched.toArray(requests);

        for (TOMMessage request : requests) {
            if (request != null && watched.remove(request) && watched.isEmpty() && rtTask != null) {
                rtTask.cancel();
                rtTask = null;
            }
        }
        rwLock.writeLock().unlock();
    }
    
    public void run_lc_protocol() {
        
        long t = getTimeout();

        if (tomLayer.isRetrievingState()) {
            int watchedCount;

            rwLock.readLock().lock();
            try {
                watchedCount = watched.size();
            } finally {
                rwLock.readLock().unlock();
            }

            logger.info(
                    "Skipping request-timeout leader-change trigger while retrieving state (watchedRequests={})",
                    watchedCount);

            rtTask = new RequestTimerTask();
            timer.schedule(rtTask, t);
            return;
        }
        
        //System.out.println("(RequestTimerTask.run) I SOULD NEVER RUN WHEN THERE IS NO TIMEOUT");

        LinkedList<TOMMessage> pendingRequests = new LinkedList<>();

        try {
        
            rwLock.readLock().lock();
        
            for (Iterator<TOMMessage> i = watched.iterator(); i.hasNext();) {
                TOMMessage request = i.next();
                if ((System.currentTimeMillis() - request.receptionTimestamp ) > t) {
                    pendingRequests.add(request);
                }
            }
            
        } finally {
            
            rwLock.readLock().unlock();
        }
        
        if (!pendingRequests.isEmpty()) {
            
            logger.info("The following requests timed out: " + pendingRequests);
            
            for (ListIterator<TOMMessage> li = pendingRequests.listIterator(); li.hasNext(); ) {
                TOMMessage request = li.next();
                if (!request.timeout) {
                    
                    // logger.info("Forwarding requests {} to leader", request);

                    request.signed = request.serializedMessageSignature != null;
                    tomLayer.forwardRequestToLeader(request);
                    request.timeout = true;
                    li.remove();
                }
            }

            if (!pendingRequests.isEmpty()) {
                logger.info("Attempting to start leader change for requests {}", pendingRequests);
                //Logger.debug = true;
                //tomLayer.requestTimeout(pendingRequests);
                //if (reconfManager.getStaticConf().getProcessId() == 4) Logger.debug = true;
                tomLayer.getSynchronizer().triggerTimeout(pendingRequests);
            }
            else {
                rtTask = new RequestTimerTask();
                timer.schedule(rtTask, t);
            }
        } else {
            
            logger.debug("Timeout triggered with no expired requests");
            
            rtTask = new RequestTimerTask();
            timer.schedule(rtTask, t);
        }
        
    }
    
    public void setSTOP(int regency, LCMessage stop) {
        synchronized (stopTimers) {
            if (regency < highestStopRegencySeen) {
                return;
            }

            if (regency > highestStopRegencySeen) {
                highestStopRegencySeen = regency;
                stopSTOPsBeforeLocked(regency);
            }

            stopSTOPLocked(regency);

            SendStopTask stopTask = new SendStopTask(stop);
            Timer stopTimer = new Timer("Stop message");

            stopTimer.schedule(stopTask, getTimeout());

            stopTimers.put(regency, stopTimer);
        }

    }   
    
    public void stopAllSTOPs() {
        synchronized (stopTimers) {
            Iterator stops = ((HashMap<Integer, Timer>) stopTimers.clone()).keySet().iterator();
            while (stops.hasNext()) {
                stopSTOPLocked((Integer) stops.next());
            }
            highestStopRegencySeen = -1;
        }
    }
    
    public void stopSTOP(int regency){
        synchronized (stopTimers) {
            stopSTOPLocked(regency);
        }

    }
    
    public Set<Integer> getTimers() {
        synchronized (stopTimers) {
            return ((HashMap<Integer, Timer>) stopTimers.clone()).keySet();
        }
        
    }

    private void stopSTOPLocked(int regency) {
        Timer stopTimer = stopTimers.remove(regency);
        if (stopTimer != null) {
            stopTimer.cancel();
        }
    }

    private void stopSTOPsBeforeLocked(int regency) {
        Iterator<Integer> timers = ((HashMap<Integer, Timer>) stopTimers.clone()).keySet().iterator();
        while (timers.hasNext()) {
            int existingRegency = timers.next();
            if (existingRegency < regency) {
                stopSTOPLocked(existingRegency);
            }
        }
    }

    private boolean shouldRetransmitStop(int regency) {
        synchronized (stopTimers) {
            if (regency < highestStopRegencySeen) {
                stopSTOPLocked(regency);
                return false;
            }
            return true;
        }
    }
    
    public void shutdown() {
        timer.cancel();
        stopAllSTOPs();
        LoggerFactory.getLogger(this.getClass()).info("RequestsTimer stopped.");

    }

    public void onViewChangeStarted() {
        long effectiveTimeout;
        long multiplier;

        synchronized (backoffLock) {
            if (!timeoutBackoffEnabled) {
                resetBackoffStateLocked();
                return;
            }

            // Legacy behavior: keep compatibility when no success-based decay policy is configured.
            if (!isSuccessBasedDecayEnabled()) {
                if (resetBackoffOnNextViewChange) {
                    backoffMultiplier = 1;
                    resetBackoffOnNextViewChange = false;
                } else {
                    incrementBackoffMultiplierLocked();
                }
            } else {
                // First timeout after successful progress keeps the current multiplier.
                // Only consecutive view-change attempts without progress increase it.
                if (resetBackoffOnNextViewChange) {
                    resetBackoffOnNextViewChange = false;
                } else {
                    incrementBackoffMultiplierLocked();
                }
            }

            effectiveTimeout = getTimeoutLocked();
            multiplier = backoffMultiplier;
        }

        logger.info("Using request timeout {} ms for view-change attempt (multiplier={})", effectiveTimeout, multiplier);
    }

    public void onViewInstalled() {
        synchronized (backoffLock) {
            if (!timeoutBackoffEnabled) {
                resetBackoffStateLocked();
                return;
            }

            if (!isSuccessBasedDecayEnabled()) {
                resetBackoffOnNextViewChange = true;
            } else if (backoffDecayTiming == BackoffDecayTiming.NEXT_VIEW && pendingDecaySteps > 0) {
                applyDecayStepsLocked(pendingDecaySteps);
                pendingDecaySteps = 0;
            }
        }

        logger.info("View installed");
    }

    public void onSequenceExecuted(int consensusId) {
        long steps = 0;
        long multiplierAfter = 1;
        boolean applyNow = false;

        synchronized (backoffLock) {
            if (!timeoutBackoffEnabled || !isSuccessBasedDecayEnabled()) {
                return;
            }

            // Any successful sequence marks recovery/progress; next view-change attempt
            // should start with current multiplier (no immediate increment).
            resetBackoffOnNextViewChange = true;

            successfulSequencesSinceDecay = safeAdd(successfulSequencesSinceDecay, 1);
            if (successfulSequencesSinceDecay < decayAfterSuccessfulSequences) {
                return;
            }

            steps = successfulSequencesSinceDecay / decayAfterSuccessfulSequences;
            successfulSequencesSinceDecay = successfulSequencesSinceDecay % decayAfterSuccessfulSequences;

            if (backoffDecayTiming == BackoffDecayTiming.CURRENT_VIEW) {
                applyDecayStepsLocked(steps);
                applyNow = true;
            } else {
                pendingDecaySteps = safeAdd(pendingDecaySteps, steps);
            }

            multiplierAfter = backoffMultiplier;
        }

        if (applyNow) {
            logger.info(
                    "Applied {} timeout-backoff decay step(s) after successful sequence {}. Multiplier={}",
                    steps,
                    consensusId,
                    multiplierAfter);
        } else {
            logger.info(
                    "Queued {} timeout-backoff decay step(s) after successful sequence {} for next view",
                    steps,
                    consensusId);
        }
    }

    private boolean isSuccessBasedDecayEnabled() {
        return decayAfterSuccessfulSequences > 0;
    }

    private void resetBackoffStateLocked() {
        backoffMultiplier = 1;
        resetBackoffOnNextViewChange = true;
        successfulSequencesSinceDecay = 0;
        pendingDecaySteps = 0;
    }

    private void incrementBackoffMultiplierLocked() {
        long updatedMultiplier;
        if (backoffIncrementMode == BackoffIncrementMode.LINEAR) {
            updatedMultiplier = safeAdd(backoffMultiplier, 1);
        } else {
            updatedMultiplier = (backoffMultiplier > Long.MAX_VALUE / 2) ? Long.MAX_VALUE : backoffMultiplier * 2;
        }
        backoffMultiplier = clampMultiplierToCapLocked(updatedMultiplier);
    }

    private void applyDecayStepsLocked(long steps) {
        for (long i = 0; i < steps; i++) {
            if (backoffMultiplier <= 1) {
                return;
            }

            if (backoffDecayMode == BackoffDecayMode.LINEAR) {
                backoffMultiplier = Math.max(1, backoffMultiplier - 1);
            } else if (backoffDecayMode == BackoffDecayMode.EXPONENTIAL) {
                backoffMultiplier = Math.max(1, backoffMultiplier / 2);
            } else {
                backoffMultiplier = 1;
                return;
            }
        }
    }

    private long safeAdd(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private long getBaseTimeoutLocked() {
        return (shortTimeout > -1 ? shortTimeout : timeout);
    }

    private long maxMultiplierForBaseLocked() {
        long base = getBaseTimeoutLocked();
        if (base <= 0) {
            return Long.MAX_VALUE;
        }
        long capped = MAX_EFFECTIVE_TIMEOUT_MS / base;
        return Math.max(1L, capped);
    }

    private long clampMultiplierToCapLocked(long multiplier) {
        return Math.max(1L, Math.min(multiplier, maxMultiplierForBaseLocked()));
    }

    private BackoffIncrementMode parseBackoffIncrementMode(String mode) {
        if ("linear".equalsIgnoreCase(mode)) {
            return BackoffIncrementMode.LINEAR;
        }
        return BackoffIncrementMode.EXPONENTIAL;
    }

    private BackoffDecayMode parseBackoffDecayMode(String mode) {
        if ("linear".equalsIgnoreCase(mode)) {
            return BackoffDecayMode.LINEAR;
        }
        if ("exponential".equalsIgnoreCase(mode)) {
            return BackoffDecayMode.EXPONENTIAL;
        }
        return BackoffDecayMode.RESET;
    }

    private BackoffDecayTiming parseBackoffDecayTiming(String timing) {
        if ("current_view".equalsIgnoreCase(timing)) {
            return BackoffDecayTiming.CURRENT_VIEW;
        }
        return BackoffDecayTiming.NEXT_VIEW;
    }
    
    class RequestTimerTask extends TimerTask {

        @Override
        /**
         * This is the code for the TimerTask. It executes the timeout for the first
         * message on the watched list.
         */
        public void run() {
            
            int[] myself = new int[1];
            myself[0] = controller.getStaticConf().getProcessId();

            communication.send(myself, new LCMessage(-1, TOMUtil.TRIGGER_LC_LOCALLY, -1, null));

        }
    }
    
    class SendStopTask extends TimerTask {
        
        private LCMessage stop;
        
        public SendStopTask(LCMessage stop) {
            this.stop = stop;
        }

        @Override
        /**
         * This is the code for the TimerTask. It sends a STOP
         * message to the other replicas
         */
        public void run() {
                if (!shouldRetransmitStop(stop.getReg())) {
                    return;
                }

                logger.info("Re-transmitting STOP message to install regency " + stop.getReg());
                communication.send(controller.getCurrentViewOtherAcceptors(),this.stop);

                setSTOP(stop.getReg(), stop); //repeat
        }
        
    }
}
