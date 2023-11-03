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
 * @create 2023-10-17 15:21
 * @desc:
 */
public class Demo12_UpsertKafkaConnectorDetail2 {
    public static void main(String[] args) throws Exception {
        /* 观察-D是如何产生的
         * 1.创建socket stream，A、B，转成table
         * 2.A 来一条数据先join不上，输出结果
         * 3.B 再来一条数据join上了，输出个更新结果
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 1,male
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9998);
        // 1,name
        DataStreamSource<String> s2 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });

        SingleOutputStreamOperator<Bean2> bean2 = s2.map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });
        // 流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);
        // inner join 只会产生+I，对于一个key而言，只会在两表都有数据的时候才会输出，输出总是先前没join过的case
        // 外join，对于join不上的case，会进行输出 补null---》+I;
        //      如果后面又能够join上了，则产生-D对冲先前的补null case，+I产生新的join 结果
        tenv.executeSql("select bean1.id, bean2.name, bean1.gender from bean1 left join bean2" +
                " on bean1.id = bean2.id ").print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2{
        public int id;
        public String name;
    }
}
