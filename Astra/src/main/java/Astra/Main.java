package Astra;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Main {

    // =============================
    // 全局窗口参数
    // =============================
    public static final long WINDOW_SIZE = 10000L;   // 10 seconds
    public static final long WINDOW_SLIDE = 1L;   // 1 seconds

    // =============================
    // Kafka 配置
    // =============================
    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String KAFKA_TOPIC = "astra-input";
    public static final String KAFKA_GROUP = "astra-group";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // =============================
        // Kafka Source
        // =============================
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(KAFKA_BOOTSTRAP)
                        .setTopics(KAFKA_TOPIC)
                        .setGroupId(KAFKA_GROUP)
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        DataStream<String> sourceStream =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (element, recordTimestamp) ->
                                                System.currentTimeMillis()),
                        "Kafka Source");

        // keyBy
        KeyedStream<String, String> dataStream =
                sourceStream.keyBy(e -> e);

        // =============================
        // Broadcast 全局参数
        // =============================
        MapStateDescriptor<String, GlobalParams> descriptor =
                new MapStateDescriptor<>(
                        "globalParams",
                        String.class,
                        GlobalParams.class);

        BroadcastStream<GlobalParams> broadcastStream =
                env.addSource(new SAPCSource())
                        .broadcast(descriptor);

        // =============================
        // 主处理逻辑
        // =============================
        dataStream
                .connect(broadcastStream)
                .process(new InactiveKeyFunction(WINDOW_SIZE, WINDOW_SLIDE))
                .returns(String.class)
                .keyBy(value ->
                        Long.parseLong(value.split("\\|")[1]))
                .process(new DownstreamProcessFunction())
                .print();

        env.execute("Astra Sliding Window Kafka Job");
    }
}