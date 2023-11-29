package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-17 15:21
 * @desc:
 */
public class Demo12_UpsertKafkaConnectorDetail {
    public static void main(String[] args) throws Exception {
        /* 观察-U、+U stream插入upsert Sink；从upsert source读时的现象
         * 1.创建socket stream，转成table
         * 2.SQL query into, connector 作为sink
         * 3.再select * from, connector source 读出来(对比之间kf数据)
         * */
        // 这种 开启webUI报异常，导致select * from table 这条sql无法执行
        //Configuration configuration = new Configuration();
        //configuration.setInteger("rest.port", 8822);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 1,male
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });
        // 流转表
        tenv.createTemporaryView("bean1", bean1);
        // tenv.executeSql("select gender, count(1) as cnt from bean1 group by gender").print();
        tenv.executeSql(
                " create table t_upsert_kafka(                                          "
                        + "   gender string primary key not enforced,                            "
                        + "   cnt bigint                                              "
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'upsert-kafka',                                        "
                        + "   'topic' = 'doit30_upsert_kafka',                                   "
                        + "   'properties.bootstrap.servers' = 'hadoop102:9092',              "
                        + "   'key.format'='csv',                               "
                        + "   'value.format'='csv'                             "
                        + " )                                                               "
        );
        // connector 作为sink时，如何写入stream？
        tenv.executeSql("insert into t_upsert_kafka select gender, count(1) as cnt" +
                        " from bean1 group by gender");
        // connector作为source时，如何恢复stream？
        tenv.executeSql("select * from t_upsert_kafka").print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }
}
