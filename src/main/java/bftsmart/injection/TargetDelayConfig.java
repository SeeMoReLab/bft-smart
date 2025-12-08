package bftsmart.injection;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TargetDelayConfig implements Serializable {
    @JsonProperty("target_node")
    private int targetNode;

    @JsonProperty("delay_bounds")
    private DelayBounds delayBounds;

    public DelayBounds getDelayBounds() {
        return delayBounds;
    }

    public int getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(int targetNode) {
        this.targetNode = targetNode;
    }

    public void setDelayBounds(DelayBounds delayBounds) {
        this.delayBounds = delayBounds;
    }
}
