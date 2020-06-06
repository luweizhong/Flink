package com.stream.flink.core;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 自定义一个operator state
 */
public class FlinkOperatorState {

    private final static String TOPIC = "test";
    private final static String GROUPID = "23";
    private final static String BOOTSTRAPSERVERS = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger(FlinkOperatorState.class.getName());

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        //设置checkpont的超时时间
        environment.getCheckpointConfig().setCheckpointTimeout(10000);
        //设置checkpoint的间隔时间
        environment.getCheckpointConfig().setCheckpointInterval(5000);
        //设置同一时刻只能有一个checkpoint
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置如果checkpoint ,如果设置为true，则任务将在检查点错误时失败。如果设置为false，则任务将仅拒绝检查点并继续运行。默认值为true。
        environment.getCheckpointConfig().setFailOnCheckpointingErrors(true);


        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);


        DataStreamSource<String> streamSource = environment.addSource(new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), properties));

        streamSource.addSink(new BatchPuts(10));
        try {
            environment.execute(FlinkOperatorState.class.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 实现一个自定义的sink
     * RichSinkFunction 继承AbstractRichFunction
     */
    public static class BatchPuts extends RichSinkFunction<String> implements CheckpointedFunction {

        private int batchSize;
        private List<String> pendingList;
        private ListState<String> listState;

        public BatchPuts(int batchSize) {
            this.batchSize = batchSize;
        }

        /**
         * 函数的初始化方法，在实际工作之前调用，适合初始化工作
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            logger.error("open....");
        }

        /**
         * 销毁方法
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            logger.error("close...");
            if (null != pendingList && pendingList.size() > 0) {
                insert(pendingList);
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            logger.error("invoke..."+value);
            pendingList.add(value);
            if (pendingList.size() >= batchSize) {
                insert(pendingList);
                pendingList.clear();
            }
            //错误程序
            if(value.contains("55")){
                System.out.println(1/0);
            }
        }

        /**
         * 每次调用checkpoint时会调用此方法
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            logger.error("snapshotState...");
            listState.clear();
            for (String put : pendingList) {
                listState.add(put);
            }
        }


        /**
         * 在创建并行函数实例时创建，用来初始化状态结果
         *
         * 在open之前
         * 在restores时需要使用的对象在该方法初始化
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            logger.error("initializeState....");
            pendingList = new ArrayList<>();
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<String>("puts", String.class);
            //创建或者还原状态
            listState = context.getOperatorStateStore().getListState(descriptor);
            //如果状态是从上一次还原的则返回true，无状态任务永远返回false
            if (context.isRestored()) {
                Iterable<String> strings = listState.get();
                for (String state : strings) {
                    pendingList.add(state);
                }
              logger.error(pendingList.toString());
            }
        }

        /**
         * 插入外部系统
         *
         * @param puts
         */
        public void insert(List<String> puts) {
            for (String put : puts) {
                logger.error("insert : " + put);
            }
        }


    }
}
