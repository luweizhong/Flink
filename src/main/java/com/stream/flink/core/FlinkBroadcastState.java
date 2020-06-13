package com.stream.flink.core;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * 场景：动态配置参数的更新
 * 广播变量动态更新配置
 * 1、定一个配置stream，用来实时发送更新的配置信息 configStream
 * 2、广播configStream，并使用broadcast state
 * 3、用源stream和broadcastStream connect，这样在connec之后下游的每个task都会有该实例
 * 4、执行BroadcastProcessFunction，分别处理processBroadcastElement和processElement
 * 5、processBroadcastElement处理广播配置文件，实时更新到状态中。processElement处理真的业务逻辑，同时结合状态中数据
 */
public class FlinkBroadcastState {

    private final static String userEvent = "user-event";
    private final static String userConfig = "user-config";

    private final static String GROUPID = "23";
    private final static String BOOTSTRAPSERVERS = "localhost:9092";

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties userEventProps = new Properties();
        userEventProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        userEventProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        DataStreamSource<String> userEventSource = environment.addSource(new FlinkKafkaConsumer<String>(userEvent, new SimpleStringSchema(), userEventProps));
        Properties userConfigProps = new Properties();
        userConfigProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        userConfigProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        DataStreamSource<String> userConfigSource = environment.addSource(new FlinkKafkaConsumer<String>(userConfig, new SimpleStringSchema(), userConfigProps));

        MapStateDescriptor descriptor = new MapStateDescriptor("user-config-broadcast", String.class, String.class);
        BroadcastStream<String> broadcastStream = userConfigSource.broadcast(descriptor);

        BroadcastConnectedStream<String, String> connectedStream = userEventSource.connect(broadcastStream);

        connectedStream.process(new BroadcastProcessFunction<String, String, Object>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastConfig = ctx.getBroadcastState(descriptor);
                for (Map.Entry<String, String> entity : broadcastConfig.immutableEntries()) {
                    String key = entity.getKey();
                    String value1 = entity.getValue();
                    System.out.println("key : " + key + " ,value : " + value1);
                }
                System.out.println("processElement..." + value);
                out.collect(value);
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Object> out) throws Exception {
                BroadcastState broadcastState = ctx.getBroadcastState(descriptor);
                JSONObject jsonObject = JSONObject.parseObject(value);
                if (!broadcastState.contains(jsonObject.getString("id"))) {
                    broadcastState.put(jsonObject.getString("id"), jsonObject.getString("name"));
                }
                System.out.println(value);
            }
        }).setParallelism(2);

        try {
            environment.execute("FlinkBroadcastState");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
