package cn.doitedu.flink.demos;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author zengwang
 * @create 2023-03-15 09:52
 * @desc:
 */
public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        /*
         * 从集合得到数据流，这里不做演示
         */
        // DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6);

        /*
         * 从文件得到数据流
         */
        //
        DataStreamSource<String> fileSource = env.readTextFile("flink_course/src/data/wc/input/wc.txt", "utf-8");
        fileSource.map(String::toUpperCase);//.print();

        // FileProcessingMode.PROCESS_ONCE: 对文件只读一次，只计算一次程序就退出
        // FileProcessingMode.PROCESS_CONTINUOUSLY： 周期性监视文件，一旦文件内容变化发生变化，就会对整个内容再读取计算一遍
        DataStreamSource<String> filSource2 = env.readFile(new TextInputFormat(null),
                "flink_course/src/data/wc/input/wc.txt",
                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        // filSource2.map(String::toUpperCase).print();

        /*
         * 从kafka中读取数据得到数据流
         */
        // 创建kafka source算子
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("tp01") // 设置订阅的目标主题
                .setGroupId("gp01") // 设置消费者id
                .setBootstrapServers("hadoop102:9092") // kafka服务器地址
                // 起始消费位移指定:
                // OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 选择先前提交的偏移量开始(如果没有则读取最新的)
                // OffsetsInitializer.earliest() 直接选择最早位置开始
                // OffsetsInitializer.latest() 直接选择最新位置开始
                // OffsetsInitializer.offsets(Map< TopicPartition, Long>
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))

                // 设置value数据的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())

                // 开启kafka底层消费者的自动位移提交机制
                //      这会把最新的消费位移提交到kafka的__consumer_offsets中，
                //      就算开启该机制，kafka source依然不会依赖kf的自动位移提交
                //      (宕机重启后，优先从Flink自己的状态中获取偏移量<更可靠>)
                .setProperty("auto.offset.commit", "true")

                // 把本source算子设置为 BOUNDED属性 (有界流)
                //      将来source去读取数据的时候，读到指定的位置，就停止读取并退出
                //      常用于补数或者重跑一段历史数据
                //.setBounded(OffsetsInitializer.committedOffsets())

                // 将本source算子设置为：UNBOUNDED属性(无界流)
                //      但是并不会一直读数据，而是读到指定位置就停止读取，但程序不退出
                //      主要应用场景: 需要从kf中读取一段固定长度的历史数据，然后拿着这段历史数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())
                .build();

        // env.addSource(); // 接受的是 SourceFunction接口的实现类
        // env.fromSource(); // 接受Source接口的实现类

        DataStreamSource<String> kfStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "kfk-source");

        kfStream.print();

        // 触发执行，运行所有sink算子的算子链
        env.execute();
    }
}
