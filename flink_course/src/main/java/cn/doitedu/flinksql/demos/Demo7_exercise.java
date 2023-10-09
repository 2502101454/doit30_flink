package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-09-23 16:35
 * @desc:
 */
public class Demo7_exercise {
    public static void main(String[] args) {
        /*
        需求：有用户信息结构 {"id": 1, "name": {"formal": "zs", "nick": "tiedan"}, "age": 30, "gender": "female"}
             从Kafka读到输入，统计：
             1.截止到当前，各个nick name的平均年龄
             2.将输出 继续写回kafka
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );
        tenv.executeSql(
                "create table t_kafka2                                  "
                        + " (                                                   "
                        + "   nick string,                         "
                        + "   avg_age float,                                         "
                        + "   primary key(nick) not enforced                                        "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'upsert-kafka',                             "
                        + "  'topic' = 'wz-flink2',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        //+ "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'key.format' = 'json',                "
                        + "  'value.format' = 'json'                                "
                        + " )                                                   "
        );
        // {"id": 1, "name": {"formal": 'zs', 'nick': 'tiedan'}, "age": 30, "gender": "female"}
        //!!!! json里面的字符串是 双引号，没有单引号的操作，不然就会解析出错，上面忽略错误，然后没有输出！！
        //System.out.println(tenv.listTables()[0].toString());
        // tenv.executeSql("select id, name['formal'], name['nick'], age, gender from t_kafka").print();
        tenv.executeSql("insert into t_kafka2 " +
                 "select name['nick'] as nick, avg(age) as avg_age from t_kafka where name['nick'] is not null " +
                 "group by name['nick']");

        // 读取的时候如何设置偏移量？ 按照自动恢复-u的机制，需要数据的完整性，因此不支持指定偏移量设置，默认就是从最早开始读
        //tenv.executeSql("select  nick, avg_age from t_kafka2 ").print();
    }
}
