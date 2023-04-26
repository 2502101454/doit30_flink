package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

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

        // 构造一个JdbcSink算子
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

        streamSource.addSink(jdbcSink);
        env.execute();
    }
}
