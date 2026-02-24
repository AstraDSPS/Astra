package Astra;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DownstreamProcessFunction
        extends KeyedProcessFunction<Long, String, String> {

    private MapState<String, String> buffer;

    @Override
    public void open(Configuration parameters) {

        buffer = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "buffer",
                        String.class,
                        String.class));
    }

    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<String> out) throws Exception {

        String[] parts = value.split("\\|");

        if (parts[0].equals("DATA")) {

            String key = parts[2];
            buffer.put(key, value);

        } else if (parts[0].equals("END")) {

            for (String key : buffer.keys()) {
                out.collect("Process: " + buffer.get(key));
            }

            buffer.clear();
        }
    }
}