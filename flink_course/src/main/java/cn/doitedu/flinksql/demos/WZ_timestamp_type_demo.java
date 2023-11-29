package cn.doitedu.flinksql.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-11-26 11:56
 * @desc:
 */
public class WZ_timestamp_type_demo {
    public static void main(String[] args) {
        /**
         * 彻底搞懂 timestamp和timestamp_ltz的区别：
         * 1.定义socket的表结构
         * 2.写入 两类字段的格式：
         * 3.select from table 字段的格式:
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // access flink configuration
        Configuration configuration = tenv.getConfig().getConfiguration();
        // set low-level key-value options
        //configuration.setString("table.local-time-zone", "America/Los_Angeles");

        tenv.executeSql(
                "create table wz_time_col                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   ts_col timestamp,                                      "
                        + "   ts_ltz_col timestamp_ltz                                "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinkSQL-time-test',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'false',                "
                        + "  'json.timestamp-format.standard' = 'SQL'                "
                        + " )                                                   "
        );
        tenv.executeSql("select * from wz_time_col").print();
        //value
        //{"id": 1, "ts_col": "2023-11-11 20:21:12.123", "ts_ltz_col": "2023-11-11 20:21:12.123Z"}
    }
}
