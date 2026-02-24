package Astra;

import java.io.Serializable;

public class Predictor implements Serializable {

    public int i = 1;
    public double S = 0.0;
    public int Np = 0;

    public boolean update(long ts, int n, int Tp) {

        if (i != n + 1) {
            S -= ts;
        } else {

            if (S + (n - 1) / 2.0 * ts > 0) {

                Np++;
                if (Np > Tp) {
                    return true;
                }

            } else {
                Np = 0;
            }

            S = (n - 1) / 2.0 * ts;
            i = 1;
        }

        i++;
        return false;
    }

    public void reset() {
        i = 1;
        S = 0;
        Np = 0;
    }
}