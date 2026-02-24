package Astra;

import java.io.Serializable;

public class LocalStats implements Serializable {

    public long N_P_to_T = 0;
    public long N_T_to_P = 0;
    public long N_re = 0;
    public long N_ac = 0;

    public void reset() {
        N_P_to_T = 0;
        N_T_to_P = 0;
        N_re = 0;
        N_ac = 0;
    }
}