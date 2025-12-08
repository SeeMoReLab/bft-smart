package bftsmart.injection;

import bftsmart.demo.map.MapMessage;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.*;
import java.util.List;

public class InjectionConfig implements Serializable {
    @JsonProperty("time")
    private int time;

    @JsonProperty("alive")
    private List<Integer> alive;

    @JsonProperty("ignore_nodes")
    private List<IgnoreNodesConfig> ignoreNodes;

    @JsonProperty("delay_config")
    private List<DelayConfig> delayConfigs;

    public List<DelayConfig> getDelayConfigs() {
        return delayConfigs;
    }

    public List<IgnoreNodesConfig> getIgnoreNodes() {
        return ignoreNodes;
    }

    public List<Integer> getAlive() {
        return alive;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void setAlive(List<Integer> alive) {
        this.alive = alive;
    }

    public void setIgnoreNodes(List<IgnoreNodesConfig> ignoreNodes) {
        this.ignoreNodes = ignoreNodes;
    }

    public void setDelayConfigs(List<DelayConfig> delayConfigs) {
        this.delayConfigs = delayConfigs;
    }

    /**
     * Serialize InjectionConfig to byte array
     */
    public static byte[] toBytes(InjectionConfig config) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
        objOut.writeObject(config);
        objOut.flush();
        byteOut.flush();
        return byteOut.toByteArray();
    }

    /**
     * Deserialize InjectionConfig from byte array
     */
    public static InjectionConfig fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        ObjectInputStream objIn = new ObjectInputStream(byteIn);
        return (InjectionConfig) objIn.readObject();
    }
}