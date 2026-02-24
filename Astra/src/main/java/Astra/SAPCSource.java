package Astra;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SAPCSource implements SourceFunction<GlobalParams> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<GlobalParams> ctx)
            throws Exception {

        GlobalParams params =
                new GlobalParams(5, 6, 3.0);

        while (running) {

            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(params);
            }

            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}