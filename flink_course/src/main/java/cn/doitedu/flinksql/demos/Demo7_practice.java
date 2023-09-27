package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author zengwang
 * @create 2023-09-23 16:35
 * @desc:
 */
public class Demo7_practice {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 环境创建之处，会地总初始化一个 元数据空间实现对象(defalut_catalog   ==> GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql(
                "create table t_kafka                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name map<string, string>,                         "
                        //+ "   name string,                         "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'wz-flink',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );
        // {"id": 1, "name": {"formal": 'zs', 'nick': 'tiedan'}, "age": 30, "gender": "female"}
        //!!!! json里面的字符串是 双引号，没有单引号的操作，不然就会解析出错，上面忽略错误，然后没有输出！！
        // {"id": 1, "name": {"formal": "zs", "nick": "tiedan"}, "age": 30, "gender": "female"}
        System.out.println(tenv.listTables()[0].toString());
        tenv.executeSql("select id, name['formal'], name['nick'], age, gender from t_kafka").print();

    }
}
