package Astra;

import java.io.Serializable;

public class MyCollector implements Serializable {

    private boolean earlyEmitted = false;
    private boolean updatedAfterEmit = false;

    public boolean isEarlyEmitted() {
        return earlyEmitted;
    }

    public void setEarlyEmitted() {
        this.earlyEmitted = true;
        this.updatedAfterEmit = false;
    }

    public void markRecovered(LocalStats stats) {

        if (!updatedAfterEmit) {
            stats.N_re++;
        }

        updatedAfterEmit = true;
    }

    public boolean shouldReEmit() {
        return earlyEmitted && updatedAfterEmit;
    }
}