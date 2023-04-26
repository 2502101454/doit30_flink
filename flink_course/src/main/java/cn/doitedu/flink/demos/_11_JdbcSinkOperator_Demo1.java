package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zengwang
 * @create 2023-04-25 23:22
 * @desc:
 * CREATE TABLE t_eventlog (
 * guid bigint NOT NULL AUTO_INCREMENT,
 * sessionId varchar(100) DEFAULT NULL,
 * eventId varchar(100) DEFAULT NULL,
 * ts bigint DEFAULT NULL,
 * eventInfo varchar(100) DEFAULT NULL,
 * PRIMARY KEY (guid)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
 */
public class _11_JdbcSinkOperator_Demo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 开启Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zeng.wang/Downloads/ckpt");

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 构造一个JdbcSink算子，不保证EOC，因为底层mysql的`producer` 即connection 没有开启事务
        SinkFunction<EventLog> jdbcSink = JdbcSink.sink("insert into t_eventlog values(?, ?, ?, ?, ?) on duplicate" +
                        " key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                        stmt.setLong(1, eventLog.getGuid());
                        stmt.setString(2, eventLog.getSessionId());
                        stmt.setString(3, eventLog.getEventId());
                        stmt.setLong(4, eventLog.getTimestamp());
                        stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
                        stmt.setString(6, eventLog.getSessionId());
                        stmt.setString(7, eventLog.getEventId());
                        stmt.setLong(8, eventLog.getTimestamp());
                        stmt.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUsername("root")
                        .withPassword("12345678")
                        .withUrl("jdbc:mysql://localhost:3306/wz_test")
                        .build()
        );

//        streamSource.addSink(jdbcSink);

        // 构造一个JdbcSink算子对象,保证EOC的一种，底层利用jdbc目标数据库的事务机制
        SinkFunction<EventLog> JdbcEocSink = JdbcSink.exactlyOnceSink("insert into t_eventlog values(?, ?, ?, ?, ?) on duplicate" +
                        " key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventLog eventLog) throws SQLException {
                        stmt.setLong(1, eventLog.getGuid());
                        stmt.setString(2, eventLog.getSessionId());
                        stmt.setString(3, eventLog.getEventId());
                        stmt.setLong(4, eventLog.getTimestamp());
                        stmt.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
                        stmt.setString(6, eventLog.getSessionId());
                        stmt.setString(7, eventLog.getEventId());
                        stmt.setLong(8, eventLog.getTimestamp());
                        stmt.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).build(),
                // mysql不支持一个连接上并行多个事务，必须设置此参数为true
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
                // XaDataSource就是Jdbc连接，不过它是支持分布式事务的连接，它的构造方法，不同的数据是不同的
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://localhost:3306/wz_test");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("12345678");
                        return xaDataSource;
                    }
                }
        );

        streamSource.addSink(JdbcEocSink);
        env.execute();
    }
}
