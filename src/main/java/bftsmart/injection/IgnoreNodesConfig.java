package bftsmart.injection;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class IgnoreNodesConfig implements Serializable {
    @JsonProperty("src_node")
    private int srcNode;

    @JsonProperty("ignored")
    private List<Integer> ignoredNodes;

    public List<Integer> getIgnoredNodes() {
        return ignoredNodes;
    }

    public int getSrcNode() {
        return srcNode;
    }

    public void setSrcNode(int srcNode) {
        this.srcNode = srcNode;
    }

    public void setIgnoredNodes(List<Integer> ignoredNodes) {
        this.ignoredNodes = ignoredNodes;
    }
}
