package com.stream.flink.core;

import com.alibaba.fastjson.JSONObject;
import com.stream.flink.entity.ItemEntity;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

/**
 * 场景：整点触发，求整点的max，如0-1h的max，1-2h的max
 * 1、设置时间语意为事件时间 eventTime
 * 2、指定事件时间抽取和checkpoint的生成
 * 3、归一化时间为整点如2020-06-01 11:00:00 2020-06-01 12:00:00
 * 4、指定时间窗口
 */
public class WholePointTrigger {
    private final static Logger logger = LoggerFactory.getLogger(JDBC2Pc.class);
    private final static String TOPIC = "test";
    private final static String GROUPID = "2123";
    private final static String BOOTSTRAPSERVERS = "localhost:9092";

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
//        environment.getCheckpointConfig().setCheckpointTimeout(10000);
//        environment.getCheckpointConfig().setCheckpointInterval(2000);
//        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), properties);
        DataStreamSource<String> streamSource = environment.addSource(consumer);

        streamSource.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            String dateStr = jsonObject.getString("date");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date parseDate = sdf.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(parseDate);
            int minute = calendar.get(Calendar.MINUTE);
            if (minute > 0) {
                calendar.add(Calendar.HOUR_OF_DAY, 1);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
            }
            String formatDate = sdf.format(calendar.getTime());
            return new ItemEntity(jsonObject.getInteger("id"), jsonObject.getString("name"), calendar.getTimeInMillis());
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ItemEntity>(Time.hours(0)) {
            @Override
            public long extractTimestamp(ItemEntity element) {
                return element.getTs();
            }
        }).keyBy("name").timeWindow(Time.hours(1)).process(new ProcessWindowFunction<ItemEntity, Object, Tuple, TimeWindow>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            @Override
            public void process(Tuple tuple, Context context, Iterable<ItemEntity> elements, Collector<Object> out) throws Exception {
                long maxId = Long.MIN_VALUE;
                Iterator<ItemEntity> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    ItemEntity next = iterator.next();
                    int id = next.getId();
                    maxId = Math.max(id, maxId);
                }
                long end = context.window().getEnd();
                StringBuilder builder = new StringBuilder();
                builder.append("windEnd : " + sdf.format(new Date(end))).append(" maxId : " + maxId);
                out.collect(builder.toString());
            }
        }).print();

        try {
            environment.execute(JDBC2Pc.class.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
