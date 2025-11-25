package bftsmart.injection;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class DelayConfig implements Serializable {
    @JsonProperty("src_node")
    private int srcNode;

    @JsonProperty("message_type")
    private String messageType;

    @JsonProperty("targets")
    private List<TargetDelayConfig> targetDelays;

    public List<TargetDelayConfig> getTargetDelays() {
        return targetDelays;
    }

    public String getMessageType() {
        return messageType;
    }

    public int getSrcNode() {
        return srcNode;
    }

    public void setSrcNode(int srcNode) {
        this.srcNode = srcNode;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public void setTargetDelays(List<TargetDelayConfig> targetDelays) {
        this.targetDelays = targetDelays;
    }
}
