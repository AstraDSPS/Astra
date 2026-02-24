package Astra;

import java.io.Serializable;

public class WindowState implements Serializable {

    public long windowStart;
    public long windowEnd;

    public Predictor predictor = new Predictor();
    public Transmitter transmitter = new Transmitter();
    public MyCollector collector = new MyCollector();
    public LocalStats stats = new LocalStats();

    public WindowState(long start, long end) {
        this.windowStart = start;
        this.windowEnd = end;
    }
}