package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo16_StreamFromOrToTable {
    public static void main(String[] args) throws Exception {
        /**
         * 流表互转
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> s1 = env.socketTextStream("hadoop103", 9999);

        SingleOutputStreamOperator<Person> s2 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Person(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]), arr[3]);
        });
        // 流转表
        tenv.createTemporaryView("person", s2); // 注册了sql表名，后续使用sql语句查询
        // Table table = tenv.fromDataStream(s2); // 得到table 对象，后续使用table API查询

        // 做查询: 每种性别中年龄最大的前3人信息
        // 方式一：将sql查询结果创建为视图，以便继续查询
        String sql1 =
                "select \n"
                    + "* \n"
                +"from (select \n"
                            +"id, \n"
                            +"name, \n"
                            +"age, \n"
                            +"gender, \n"
                            +"row_number() over(partition by gender order by age desc) as rn \n"
                        +"from person \n"
                    + ") as t \n"
                + "where t.rn <=3 \n";
        // topN SQL 支持RetractStream
        // tenv.executeSql(sql1).print();

        // Table table = tenv.sqlQuery(sql1);
        // tenv.createTemporaryView("topN", table);

        // 方式二：使用sql语句来创建视图
        String sql2 =
                "create temporary view topN as " +
                        "select \n"
                        + "* \n"
                        +"from (select \n"
                        +"id, \n"
                        +"name, \n"
                        +"age, \n"
                        +"gender, \n"
                        +"row_number() over(partition by gender order by age desc) as rn \n"
                        +"from person \n"
                        + ") as t \n"
                        + "where t.rn <=3 \n";
        tenv.executeSql(sql2);
        // 继续筛选年龄奇数的（依然保留输出上游sql的-u、+u，但是要过滤偶数，其实这些sql都会拼到一起的？）
         tenv.executeSql("select * from topN where age % 2 = 1").print();

        /* 如下 代码执行时会包异常: 说socket端口连接失败，但是只跑上面的代码却可以成功，看来在复杂嵌套子查询下，
           flink sql内部已经把自己的不支持变成异常混乱的处理了，直接让任务失败了

           tenv.executeSql("create temporary view topN_odd as select * from topN where age % 2 = 1");
        // 继续统计，在这各个性别的年龄前三名的奇数记录中，求各个性别的最大年龄
           tenv.executeSql("select gender, max(age) as max_age from topN_odd group by gender");
         */

        // 将topN查询结果写如Kafka，来复现upsert-kafka会产生-D(在null下)
        // 复现不出来，写kafka都报socket异常
        // 创建目标 kafka映射表
        //tenv.executeSql(
        //        " create table t_upsert_kafka(                 "
        //                + "    id int   ,        "
        //                + "    name string,          "
        //                + "    age int,                          "
        //                + "    gender string ,                            "
        //                + "    rn bigint    ,                       "
        //                + "    primary key(gender,rn) NOT ENFORCED         "
        //                + " ) with (                                    "
        //                + "  'connector' = 'upsert-kafka',              "
        //                + "  'topic' = 'doit30-topn',                "
        //                + "  'properties.bootstrap.servers' = 'hadoop102:9092',  "
        //                + "  'key.format' = 'csv',                             "
        //                + "  'value.format' = 'csv'                            "
        //                + " )                                                  "
        //);
        //
        //tenv.executeSql("insert into t_upsert_kafka select * from topN");
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person{
        private int id;
        private String name;
        private int age;
        private String gender;
    }
}
