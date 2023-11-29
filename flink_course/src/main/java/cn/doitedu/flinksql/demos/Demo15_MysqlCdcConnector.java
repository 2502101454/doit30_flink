package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo15_MysqlCdcConnector {
    public static void main(String[] args) throws Exception {
        /**
         * 测试cdc connector的读取机制
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 建表映射mysql中的flinktest.stu
        tenv.executeSql("CREATE TABLE flink_score (\n" +
                "      id INT,\n" +
                "      name string,\n" +
                "      gender string,\n" +
                "      score double,\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'hadoop102',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'hadoop',\n" +
                "     'database-name' = 'flinktest',\n" +
                "     'table-name' = 'stu_score'\n" +
                ")");
        //tenv.executeSql("select * from flink_score").print();

        tenv.executeSql(
                " create table t_upsert_kafka(                 "
                        + "    id int   ,        "
                        + "    name string,          "
                        + "    gender string ,                            "
                        + "    score double,                          "
                        + "    primary key(id) NOT ENFORCED         "
                        + " ) with (                                    "
                        + "  'connector' = 'upsert-kafka',              "
                        + "  'topic' = 'upsert_kafka_D',                "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092',  "
                        + "  'key.format' = 'csv',                             "
                        + "  'value.format' = 'csv'                            "
                        + " )                                                  "
        );
        // 将cdc的变化流写入kafka，想要测试upsert-kafka对-D 分区咋存？
        // 实验结果：key字段存储 pk，value字段写入null
        tenv.executeSql("insert into t_upsert_kafka select * from flink_score");
        tenv.executeSql("select * from t_upsert_kafka").print();
        env.execute();
    }
}
