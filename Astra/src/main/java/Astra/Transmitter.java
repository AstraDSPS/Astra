package Astra;

import java.io.Serializable;

public class Transmitter implements Serializable {

    public int i = 1;
    public double S = 0;

    public long lastTimestamp = -1;
    public long lastGap = -1;

    public boolean tupleProcess(long ts, int n, LocalStats stats) {

        if (i != n + 1) {
            S -= ts;
        } else {

            if (S + (n - 1)/2.0 * ts < 0) {

                stats.N_T_to_P++;
                reset();
                return true;

            } else {

                S = (n - 1)/2.0 * ts;
                i = 1;
            }
        }

        i++;

        if (lastTimestamp > 0)
            lastGap = ts - lastTimestamp;

        lastTimestamp = ts;

        return false;
    }

    public boolean shouldTransmit(long currentTime, double Tg, LocalStats stats) {

        if (lastGap <= 0) {
            stats.N_ac++;
            return false;
        }

        if ((double)(currentTime - lastTimestamp) / lastGap > Tg) {
            return true;
        }

        stats.N_ac++;
        return false;
    }

    public void reset() {
        i = 1;
        S = 0;
        lastTimestamp = -1;
        lastGap = -1;
    }
}