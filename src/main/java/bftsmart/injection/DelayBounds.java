package bftsmart.injection;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class DelayBounds implements Serializable {
    @JsonProperty("min_delay")
    private int minDelay;
    @JsonProperty("max_delay")
    private int maxDelay;

    public void setMinDelay(int minDelay) {
        this.minDelay = minDelay;
    }

    public void setMaxDelay(int maxDelay) {
        this.maxDelay = maxDelay;
    }

    public int getMinDelay() { return minDelay; }
    public int getMaxDelay() { return maxDelay; }
}
