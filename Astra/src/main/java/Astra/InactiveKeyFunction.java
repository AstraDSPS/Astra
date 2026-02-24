package Astra;

import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class InactiveKeyFunction extends
        KeyedBroadcastProcessFunction<
                String,
                String,
                GlobalParams,
                String> {

    private final long windowSize;
    private final long slideStep;

    private MapState<Long, WindowState> windows;

    private final MapStateDescriptor<String, GlobalParams> broadcastDesc =
            new MapStateDescriptor<>(
                    "globalParams",
                    String.class,
                    GlobalParams.class);

    public InactiveKeyFunction(long windowSize, long slideStep) {
        this.windowSize = windowSize;
        this.slideStep = slideStep;
    }

    @Override
    public void open(Configuration parameters) {

        windows = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "windows",
                        Long.class,
                        WindowState.class));
    }

    @Override
    public void processElement(
            String value,
            ReadOnlyContext ctx,
            Collector<String> out) throws Exception {

        long ts = ctx.timestamp();

        GlobalParams params =
                ctx.getBroadcastState(broadcastDesc).get("params");

        if (params == null) return;

        long base = ts - ts % slideStep;

        for (long start = base - windowSize + slideStep;
             start <= base;
             start += slideStep) {

            if (start < 0) continue;

            long end = start + windowSize;

            WindowState ws = windows.get(start);
            if (ws == null) {
                ws = new WindowState(start, end);
                windows.put(start, ws);
                ctx.timerService().registerEventTimeTimer(end);
            }

            if (ws.collector.isEarlyEmitted()) {
                ws.collector.markRecovered(ws.stats);
            }

            boolean predicted =
                    ws.predictor.update(ts, params.n, params.Tp);

            if (predicted) {
                ws.stats.N_P_to_T++;
                ws.transmitter = new Transmitter();
            }

            boolean back =
                    ws.transmitter.tupleProcess(
                            ts,
                            params.n,
                            ws.stats);

            if (back) {
                ws.predictor.reset();
            }

            if (ws.transmitter.shouldTransmit(
                    ts,
                    params.Tg,
                    ws.stats)) {

                out.collect("DATA|" + start + "|" + ctx.getCurrentKey());
                ws.collector.setEarlyEmitted();
                ws.transmitter.reset();
            }

            windows.put(start, ws);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<String> out) throws Exception {

        Iterator<Map.Entry<Long, WindowState>> it =
                windows.entries().iterator();

        while (it.hasNext()) {

            Map.Entry<Long, WindowState> entry = it.next();
            WindowState ws = entry.getValue();

            if (ws.windowEnd == timestamp) {

                if (ws.collector.shouldReEmit()) {
                    out.collect("DATA|" + ws.windowStart + "|" + ctx.getCurrentKey());
                }

                out.collect("END|" + ws.windowStart);

                it.remove();
            }
        }
    }

    @Override
    public void processBroadcastElement(
            GlobalParams value,
            Context ctx,
            Collector<String> out) throws Exception {

        ctx.getBroadcastState(broadcastDesc)
                .put("params", value);
    }
}