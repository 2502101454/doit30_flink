package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-03 14:41
 * @desc:
 */
public class Demo9_JsonFormat {
    public static void main(String[] args) {
        /* 一条json的结构如下，由map、row、array[map or row]
         * {"id": 1, "name": "wz", "score": {"math":89, "english": 92}, "info":{"city": "xi'an", "age": 16},
         * "friends": [{"name": "lihua", "gender": "male"}, {"name": "xiaihong", "gender": "female"}]}
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 使用sql 的方式
        tenv.executeSql("create table t_student ( "
                + "id int, "
                + "name string, "
                + "score map<string, string>, "
                + "info row<city string, age int>, "
                + "friends array<row<name string, gender string>> " // 能用map就能用row，row比map好在字段类型可以不统一
                + ")with(                                      "
                + " 'connector' = 'filesystem',                "
                // + " 'path' = 'E:\\IDEAProjects\\doit30_flink\\flink_course\\src\\data\\json\\qiantao\\',             "
                + " 'path' = 'data/json/qiantao',             " // classpath是夫工程的base path
                + " 'format'='json'                            "
                + ")"
        );
        /*
        * +---------+--------------------------------------------+------+-----+--------+-----------+
        |    name |                                       type | null | key | extras | watermark |
        +---------+--------------------------------------------+------+-----+--------+-----------+
        |      id |                                        INT | true |     |        |           |
        |    name |                                     STRING | true |     |        |           |
        |   score |                        MAP<STRING, STRING> | true |     |        |           |
        |    info |              ROW<`city` STRING, `age` INT> | true |     |        |           |
        | friends | ARRAY<ROW<`name` STRING, `gender` STRING>> | true |     |        |           |
        +---------+--------------------------------------------+------+-----+--------+-----------+
        * */
        tenv.executeSql("desc t_student").print();
        tenv.executeSql("select name, score['math'] as math_socre, " +  // map只能map['field']
                "info['city'] as city, info.age as age," +      // row的取值方式row['field']、row.field 都可以
                "friends[1].name as f1_name, friends[2].name as f2_name from t_student").print();
    }
}
