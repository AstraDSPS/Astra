package Astra;

import java.io.Serializable;

public class GlobalParams implements Serializable {

    public int n;
    public int Tp;
    public double Tg;

    public GlobalParams(){}

    public GlobalParams(int n, int Tp, double Tg) {
        this.n = n;
        this.Tp = Tp;
        this.Tg = Tg;
    }
}