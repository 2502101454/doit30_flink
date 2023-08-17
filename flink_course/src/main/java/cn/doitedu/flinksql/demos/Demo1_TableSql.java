package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author zengwang
 * @create 2023-08-01 20:01
 * @desc:
 */
public class Demo1_TableSql {
    public static void main(String[] args) {

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                                                            .inStreamingMode()
                                                            .build();
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        // 动态表 结果一直在变化，对于下游而言无法理解，因此引入
        // 为啥我觉得挺好理解的，反正读啥就是最新的呗？下游先前读了(aa,1) 现在读到(aa, 2),需要告诉下游，先前的结果
        // 过期了，也许先前已经产生了一些影响，我们想要去纠正先前产生的影响？

        // 创建Kafka 表，统计不同性别的评价年龄，观察RowKind 的变化
        // json: {"id": 1, "name": "zs", "age": 26, "gender": "male"}
        // RowKind，+I、-U（代表之前的算错了）、+U(更正最新结果)、-D没见过
        tableEnv.executeSql(
               "create table t_kafka                                  "
                + " (                                                   "
                + "   id int,                                           "
                + "   name string,                                      "
                + "   age int,                                          "
                + "   gender string                                     "
                + " )                                                   "
                + " WITH (                                              "
                + "  'connector' = 'kafka',                             "
                + "  'topic' = 'doit30-3',                              "
                + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                + "  'properties.group.id' = 'g1',                      "
                + "  'scan.startup.mode' = 'earliest-offset',           "
                + "  'format' = 'json',                                 "
                + "  'json.fail-on-missing-field' = 'false',            "
                + "  'json.ignore-parse-errors' = 'true'                "
                + " )                                                   "
        );

        // 一条条查询的结果，在数据流里面被封装成一个row对象，row里面有个属性叫做RowKind，只是打印出来就是op
        // 运行机制：来一条数据计算一次，但底层不是全量的？而是增量聚合？每来一条新数据，增量计算出结果然后输出，print每次是往下追加一行输出？
        tableEnv.executeSql("select gender, avg(age) as avg_age from t_kafka group by gender").print();


        // sql表名转table对象
        // Table table = tableEnv.from("t_kafka");
        // 利用table api进行查询计算
        /*table.groupBy($("gender"))
                .select($("gender"), $("age").avg())
                .execute()
                .print();
        */



    }
}
