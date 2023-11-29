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
public class Demo15_MysqlCdcConnector2 {
    public static void main(String[] args) throws Exception {
        /**
         * 测试cdc connector读取机制 对复杂sql的支持，产生变化流
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
        //tenv.executeSql("select gender, avg(score) as avg_score from flink_score" +
        //        " group by gender").print();

        // 算出每个人的总分数，求各个年龄中总分数最大的前两人
        // 输入 每个人有多条分数记录
        tenv.executeSql(
                " SELECT\n" +
                        " gender,\n" +
                        " name,\n" +
                        " score_amt,\n" +
                        " row_number() over(partition by gender order by score_amt desc) as rn\n" +
                        " from \n" +
                        "(\n" +
                        "SELECT\n" +
                        "gender,\n" +
                        "name,\n" +
                        "sum(score) as score_amt\n" +
                        "from flink_score\n" +
                        "group by gender,name\n" +
                        ") o1\n"
        ).print();
        //env.execute();
    }
}
