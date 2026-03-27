package bftsmart.communication.server;

import bftsmart.communication.SystemMessage;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.util.TOMUtil;

/**
 * Immutable outbound message container used by per-peer sender queues.
 * Carries the serialized payload plus lightweight metadata for dequeue-time logs.
 */
final class OutboundServerMessage {

    private enum Kind {
        LEADER_CHANGE,
        STATE_TRANSFER,
        OTHER
    }

    private final byte[] payload;
    private final Kind kind;
    private final int sender;
    private final int messageType;
    private final int regency;
    private final int cid;
    private final int leader;

    private OutboundServerMessage(byte[] payload, Kind kind, int sender, int messageType, int regency, int cid, int leader) {
        this.payload = payload;
        this.kind = kind;
        this.sender = sender;
        this.messageType = messageType;
        this.regency = regency;
        this.cid = cid;
        this.leader = leader;
    }

    static OutboundServerMessage from(byte[] payload, SystemMessage message) {
        if (message instanceof LCMessage) {
            LCMessage lc = (LCMessage) message;
            return new OutboundServerMessage(payload, Kind.LEADER_CHANGE, message.getSender(), lc.getType(), lc.getReg(), -1, -1);
        }
        if (message instanceof SMMessage) {
            SMMessage sm = (SMMessage) message;
            return new OutboundServerMessage(payload, Kind.STATE_TRANSFER, message.getSender(), sm.getType(), sm.getRegency(), sm.getCID(), sm.getLeader());
        }
        return new OutboundServerMessage(payload, Kind.OTHER, message.getSender(), -1, -1, -1, -1);
    }

    static OutboundServerMessage raw(byte[] payload) {
        return new OutboundServerMessage(payload, Kind.OTHER, -1, -1, -1, -1, -1);
    }

    byte[] payload() {
        return payload;
    }

    boolean shouldLogOnDequeue() {
        return kind == Kind.LEADER_CHANGE || kind == Kind.STATE_TRANSFER;
    }

    boolean shouldPreserveAcrossReconnect() {
        if (kind != Kind.STATE_TRANSFER) {
            return false;
        }
        switch (messageType) {
            case TOMUtil.SM_REQUEST:
            case TOMUtil.SM_REPLY:
            case TOMUtil.SM_ASK_INITIAL:
            case TOMUtil.SM_REPLY_INITIAL:
                return true;
            default:
                return false;
        }
    }

    boolean isStateTransferRequest() {
        return kind == Kind.STATE_TRANSFER && messageType == TOMUtil.SM_REQUEST;
    }

    int getCid() {
        return cid;
    }

    String describeForDequeueLog() {
        if (kind == Kind.LEADER_CHANGE) {
            return "leader-change type=" + lcMessageTypeName(messageType) + " regency=" + regency + " sender=" + sender;
        }
        if (kind == Kind.STATE_TRANSFER) {
            return "state-transfer type=" + stateMessageTypeName(messageType) + " cid=" + cid + " regency=" + regency + " leader=" + leader + " sender=" + sender;
        }
        return "system-message sender=" + sender;
    }

    private static String lcMessageTypeName(int type) {
        switch (type) {
            case TOMUtil.STOP:
                return "STOP";
            case TOMUtil.STOPDATA:
                return "STOPDATA";
            case TOMUtil.SYNC:
                return "SYNC";
            default:
                return "UNKNOWN(" + type + ")";
        }
    }

    private static String stateMessageTypeName(int type) {
        switch (type) {
            case TOMUtil.SM_REQUEST:
                return "SM_REQUEST";
            case TOMUtil.SM_REPLY:
                return "SM_REPLY";
            case TOMUtil.SM_ASK_INITIAL:
                return "SM_ASK_INITIAL";
            case TOMUtil.SM_REPLY_INITIAL:
                return "SM_REPLY_INITIAL";
            default:
                return "UNKNOWN(" + type + ")";
        }
    }
}
