package Astra;

import java.io.Serializable;
import java.util.Random;

public class SAPCOptimizer implements Serializable {

    private double T = 10;
    private final double Tmin = 0.1;
    private final double r = 0.9;
    private final int I = 5;

    private final Random rand = new Random();

    public GlobalParams optimize(
            GlobalParams old,
            LocalStats globalStats) {

        int n = old.n;
        double Tg = old.Tg;

        while (T > Tmin) {

            for (int i = 0; i < I; i++) {

                int newN = Math.max(2, n + rand.nextInt(3) - 1);
                double newTg = Math.max(1.0, Tg + rand.nextDouble() - 0.5);

                double C_old = score(n, Tg, globalStats);
                double C_new = score(newN, newTg, globalStats);

                double diff = C_new - C_old;

                if (diff > 0 ||
                        rand.nextDouble() < Math.exp(diff / T)) {

                    n = newN;
                    Tg = newTg;
                }
            }

            T *= r;
        }

        T = 10;

        return new GlobalParams(n, old.Tp, Tg);
    }

    private double score(int n, double Tg, LocalStats stats) {

        double Cn = (1 + 0.06) * stats.N_P_to_T
                - stats.N_T_to_P;

        double CTg = 1.0 /
                (1 + stats.N_re + stats.N_ac);

        return Cn + CTg;
    }
}