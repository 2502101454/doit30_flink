package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

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
public class _12_RedisSinkOperator_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);
        // 开启Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zeng.wang/Downloads/ckpt");

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

//        streamSource.print();
        // eventLog数据插入redis
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();
        // 构建redisSink算子
        RedisSink<EventLog> redisSink = new RedisSink<>(config, new HashInsertMapper());
        streamSource.addSink(redisSink);

        env.execute();
    }

    /**
     * String 类型写入
     */
    static class StringInsertMapper implements RedisMapper<EventLog> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 数据存String类型
            return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         * 指定要插入的大key，redis的大 key数量是没有上限，list、map这类其中要存的元素个数也没有上限，只要内存足够
         * @param eventLog
         * @return
         */
        @Override
        public String getKeyFromData(EventLog eventLog) {
            return eventLog.getSessionId();
        }

        /**
         * 指定大key对应的值
         * @param eventLog
         * @return
         */
        @Override
        public String getValueFromData(EventLog eventLog) {
            return JSON.toJSONString(eventLog);
        }
    }

    /**
     * Hash类型写入
     */
    static class HashInsertMapper implements RedisMapper<EventLog> {
        /**
         * 对于内部有小key的结构，比如hash，RedisCommandDescription 中写死了大key，
         * 如果需要对每一条数据，自己做一个大key，则重写这里的方法，这里的key优先级比RedisCommandDescription 中的高
         * @param data
         * @return
         */
        @Override
        public Optional<String> getAdditionalKey(EventLog data) {
            return Optional.of(data.getSessionId());
//            return RedisMapper.super.getAdditionalKey(data);
        }

        // 根据具体时间，设置不同的ttl(time to live 存活时长)
        @Override
        public Optional<Integer> getAdditionalTTL(EventLog data) {
            return RedisMapper.super.getAdditionalTTL(data);
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "event-logs");
        }

        /**
         * 1.对于有内部 小key的数据结构，比如hash，则大key由上面RedisCommandDescription or getAdditionalKey指定
         *      小key在此处指定
         *
         * 2.对于内部 没有小key的，比如list、set，则大key由此处指定，上面指定也无效
         * @param eventLog
         * @return
         */
        @Override
        public String getKeyFromData(EventLog eventLog) {
            return eventLog.getSessionId();
        }

        @Override
        public String getValueFromData(EventLog eventLog) {
            return JSON.toJSONString(eventLog); // 小key的value
        }
    }
}
