package cn.doitedu.flink.demos;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

/**
 * @author zengwang
 * @create 2023-07-10 17:06
 * @desc: Flink 端到端精确一次性 容错测试
 *
 * 输入： kafka算子读数据(其中有operator-State 记录消费位置)
 * 处理：使用keyedState，逻辑：输入一个字符，将其变大写拼接上此前的字符，进行输出
 * 输出: 使用支持eos的mysql sink算子输出(并附带 主键幂等性)
 *
 * 测试用例：
 *  kafka-topics.sh --create --topic eos --partitions 1 --replication-factor 1 --zookeeper hadoop102:2181

    CREATE TABLE `t_eos` (
    `str` varchar(255) CHARACTER SET utf8mb4 NOT NULL,
    PRIMARY KEY (`str`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

 */
public class _28ToleranceSideToSideTest {
    public static void main(String[] args) throws Exception {
        // 在代码中制造一定改了的异常，观察Task自动重启，Job手动重启时，数据从端到端的一致性！

        // --> 测试demo： 》x》b》a， 遇到x的时候如果抛异常了，下次重启恢复，还会从x重新读
        // --> sink端的异常测试，需要自己复制改源码，在其二阶段的周期方法中，自己加异常测试
        // --> 说好的job手动恢复呢？
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // env.setParallelism(1); // 方便观察现象
        /**
         * checkpoint 相关参数设置
         */
        env.enableCheckpointing(1000 * 10, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink_ckpt");

        /**
         * task 级别故障恢复，自动重启策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(1000)));

        /**
         * 构造一个支持EOS的kafkaSource
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("eos")
                .setGroupId("eos01")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 因为算子内的状态维护offset，下面两个没必要提交offset给kafka的参数（就算提交了，也只是方便外部系统监控而已）
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // 1.允许kafka consumer自动提交偏移量
                .setProperty("commit.offsets.on.checkpoint", "false") // 2.在做checkpoint时候，向kafka提交偏移量
                // 优先从算子的状态中记录的offset开始消费，如果没有，则用LATEST
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
        /**
         * 构造一个支持EOS的JDBC Sink
         */
        SinkFunction<String> jdbcSink = JdbcSink.exactlyOnceSink("insert into t_eos values (?) on duplicate key update str = ?",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                        preparedStatement.setString(2, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持一个连接上存在多个未完成的事务，必须把此参数设为false
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource 就是jdbc连接，不过它时支持分布式事务的连接，而且不同的数据库，构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://hadoop102:3306/wz_test");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("hadoop");
                        return xaDataSource;
                    }
                }
        );

        // 需求不涉及窗口计算、事件时间，因此这里不设置水位线
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");

        SingleOutputStreamOperator<String> stream = source.keyBy(s -> "group1")
                .map(new RichMapFunction<String, String>() {
                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preStr", String.class));
                    }

                    @Override
                    public String map(String element) throws Exception {
                        // 从状态中取上一次结果
                        String preStr = this.valueState.value();
                        if (preStr == null) preStr = "";

                        // 更新状态
                        valueState.update(element);

                        // 造一个异常，当接收到x时，[1, 3) 有1/2的概率异常
                        if (element.equals("x") && RandomUtils.nextInt(1, 3) % 2 == 0)
                            throw new Exception("发生异常了.........................");
                        return preStr + ":" + element.toUpperCase();
                    }
                });

        stream.addSink(jdbcSink);
        env.execute();
    }
}
