package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo13_JdbcConnectorTest2 {
    public static void main(String[] args) throws Exception {
        /**
         * 测试JDBC connector作为sink时，upsert模式的写入机制
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 建表映射mysql中的flinktest.stu2
        tenv.executeSql(
                "create table flink_stu(\n" +
                        "   id  int primary key,\n" +
                        "   name string,\n" +
                        "   gender string\n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://hadoop102:3306/flinktest',\n" +
                        "  'table-name' = 'stu2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'hadoop' \n" +
                        ")"
        );
        // 1,male
        SingleOutputStreamOperator<Bean1> bean1 = env
                .socketTextStream("hadoop102", 9998)
                .map(s -> {
                    String[] arr = s.split(",");
                    return new Bean1(Integer.parseInt(arr[0]), arr[1]);
                });
        // 1,zs
        SingleOutputStreamOperator<Bean2> bean2 = env.socketTextStream("hadoop102", 9999).map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });


        // 流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);

        tenv.executeSql("insert into flink_stu " +
                " select bean1.id, bean2.name, bean1.gender from bean1 left join bean2 on bean1.id = bean2.id");

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        public int id;
        public String gender;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2 {
        public int id;
        public String name;
    }
}
