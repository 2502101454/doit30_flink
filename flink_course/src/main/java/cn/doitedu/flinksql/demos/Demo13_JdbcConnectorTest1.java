package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo13_JdbcConnectorTest1 {
    public static void main(String[] args) throws Exception {
        /**
         * 测试JDBC connector作为source时，scan模式的读取机制
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 建表映射mysql中的flinktest.stu
        tenv.executeSql(
                "create table flink_stu(\n" +
                        "   id  int primary key,\n" +
                        "   name string,\n" +
                        "   age int,\n" +
                        "   gender string\n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://hadoop102:3306/flinktest',\n" +
                        "  'table-name' = 'stu',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'hadoop' \n" +
                        ")"
        );
        DataStreamSource<String> doitedu = env.socketTextStream("hadoop102", 9999);
        tenv.executeSql("select * from flink_stu").print();
        doitedu.print();

        env.execute();
    }
}
