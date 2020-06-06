package com.stream.flink.core;

import com.alibaba.fastjson.JSONObject;
import com.stream.flink.entity.ItemEntity;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;

public class DisOrderData {

    private final static String TOPIC = "test";
    private final static String GROUPID = "23";
    private final static String BOOTSTRAPSERVERS = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger(FlinkOperatorState.class.getName());


    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        //设置checkpont的超时时间
//        environment.getCheckpointConfig().setCheckpointTimeout(10000);
//        //设置checkpoint的间隔时间
//        environment.getCheckpointConfig().setCheckpointInterval(5000);
//        //设置同一时刻只能有一个checkpoint
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //设置如果checkpoint ,如果设置为true，则任务将在检查点错误时失败。如果设置为false，则任务将仅拒绝检查点并继续运行。默认值为true。
//        environment.getCheckpointConfig().setFailOnCheckpointingErrors(true);
//
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(600);


        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC,
                new SimpleStringSchema(),
                properties);
        kafkaConsumer.setStartFromLatest();

        DataStreamSource<String> streamSource = environment.addSource(kafkaConsumer);
        streamSource.print();
        SingleOutputStreamOperator<ItemEntity> sum = streamSource.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return new ItemEntity(jsonObject.getInteger("id"), jsonObject.getString("name"), jsonObject.getLong("ts"));
        }).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(Time.seconds(2))).timeWindowAll(Time.seconds(5)).sum("id");
        sum.map(line->{
            int id = line.getId();
            System.out.println(id);
            return null;
        });
        try {
            environment.execute(FlinkOperatorState.class.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 带断点的watermark，可以根据事件来生成watermark，
     * 首先调用extractTimestamp生成一个时间戳，立即调用checkAndGetNextWatermark返回一个非空的watermark，并且这个watermark大于或等于前一个watermark
     *
     */
    public static class MyAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<ItemEntity> {


        private long lastEmittedWatermark;
        private long maxOutOfOrderness;

        public MyAssignerWithPunctuatedWatermarks(Time maxOutOfOrderness) {
            if(maxOutOfOrderness.toMilliseconds() < 0 ){
                throw new RuntimeException("Tried to set the maximum allowed " +
                        "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
            }
            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        }

        /**
         * 生成watermark，每次调用extractTimestamp 函数之后立即调用该函数
         * 1、单调递增
         * 2、不能为空
         * @param lastElement
         * @param extractedTimestamp
         * @return
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(ItemEntity lastElement, long extractedTimestamp) {
            long max = Math.max(lastEmittedWatermark, extractedTimestamp-maxOutOfOrderness);
            lastEmittedWatermark = max;
            System.out.println("lastEmittedWatermark : " + lastEmittedWatermark);
            return new Watermark(lastEmittedWatermark);
        }

        /**
         * 提取时间戳
         * @param element
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(ItemEntity element, long previousElementTimestamp) {
            return element.getTs();
        }
    }


    /**
     * AssignerWithPeriodicWatermarks：周期性生成watermark，setAutoWatermarkInterval设置周期，默认时200ms
     *
     */
    public static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ItemEntity>{

        private long lastEmittedWatermark = Long.MIN_VALUE;
        private long maxOutOfOrderness;
        private long currentMaxTimestamp;

        public MyAssignerWithPeriodicWatermarks(Time maxOutOfOrderness){
            if(maxOutOfOrderness.toMilliseconds() < 0){
                throw new RuntimeException("Tried to set the maximum allowed " +
                        "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
            }
            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
            currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;

        }

        /**
         * 周期生成watermark
         * @return
         */
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            long max = Math.max(lastEmittedWatermark, currentMaxTimestamp-maxOutOfOrderness);
            lastEmittedWatermark = max;
            return new Watermark(lastEmittedWatermark);
        }

        /**
         * 时间触发，流入一条数据触发一次timestamp提取
         * @param element
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(ItemEntity element, long previousElementTimestamp) {
            long currentTimestamp = element.getTs();
            if(currentTimestamp > currentMaxTimestamp){
                this.currentMaxTimestamp = currentTimestamp;
            }
            return currentTimestamp;
        }
    }
}
