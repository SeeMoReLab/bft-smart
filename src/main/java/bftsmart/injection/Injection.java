package bftsmart.injection;

import bftsmart.consensus.messages.MessageFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Injection implements Serializable {
    private boolean isNodeAlive;
    private HashMap<Integer, Boolean> ignoreNodes;
    private HashMap<Integer, HashMap<Integer, DelayBounds>> delays;

    public Injection() {
        isNodeAlive = true;

        // Map of NodeId -> true/false for easy lookup
        ignoreNodes = new HashMap<Integer, Boolean>();

        // MsgType -> {Target Node ID -> Delay Bounds}
        delays = new HashMap<Integer, HashMap<Integer, DelayBounds>>();
    }

    public HashMap<Integer, Boolean> getIgnoreNodes() {
        return ignoreNodes;
    }

    public void addIgnoredNode(int ignoredNode) {
        this.ignoreNodes.put(ignoredNode, true);
    }

    public boolean isNodeAlive() {
        return isNodeAlive;
    }

    public void setNodeStatus(boolean nodeStatus) {
        isNodeAlive = nodeStatus;
    }

    public HashMap<Integer, HashMap<Integer, DelayBounds>> getDelays() {
        return delays;
    }

    public void addDelay(String msgType, int targetNode, DelayBounds delayBounds) {
        msgType = msgType.toLowerCase();
        int msgTypeInt = -1;
        System.out.println("ADDING DELAY FOR MESSAGE TYPE: " + msgType);
        switch (msgType) {
            case "propose":
                msgTypeInt = MessageFactory.PROPOSE;
                break;
            case "accept":
                msgTypeInt = MessageFactory.ACCEPT;
                break;
            case "write":
                msgTypeInt = MessageFactory.WRITE;
                break;
        }

        if (msgTypeInt == -1) return;

        HashMap<Integer, DelayBounds> targetMap = delays.computeIfAbsent(msgTypeInt, k -> new HashMap<>());

        // Set the delay bounds for the target node
        targetMap.put(targetNode, delayBounds);
    }

//    public static byte[] toBytes(Injection injection) throws IOException {
//        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
//        objOut.writeObject(injection);
//
//        objOut.flush();
//        byteOut.flush();
//        return byteOut.toByteArray();
//    }
//
//    public static Injection fromBytes(byte[] rep) throws IOException, ClassNotFoundException {
//        ByteArrayInputStream byteIn = new ByteArrayInputStream(rep);
//        ObjectInputStream objIn = new ObjectInputStream(byteIn);
//        return (Injection) objIn.readObject();
//    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Injection{");
        sb.append("alive=").append(isNodeAlive);
        sb.append(", ignoreNodes=").append(ignoreNodes);
        sb.append(", delays={");

        boolean first = true;
        for (Map.Entry<Integer, HashMap<Integer, DelayBounds>> msgEntry : delays.entrySet()) {
            if (!first) sb.append(", ");
            sb.append("msgType").append(msgEntry.getKey()).append("={");

            boolean firstTarget = true;
            for (Map.Entry<Integer, DelayBounds> targetEntry : msgEntry.getValue().entrySet()) {
                if (!firstTarget) sb.append(", ");
                sb.append("node").append(targetEntry.getKey())
                        .append(":[").append(targetEntry.getValue().getMinDelay())
                        .append(",").append(targetEntry.getValue().getMaxDelay()).append("]");
                firstTarget = false;
            }
            sb.append("}");
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }
}
