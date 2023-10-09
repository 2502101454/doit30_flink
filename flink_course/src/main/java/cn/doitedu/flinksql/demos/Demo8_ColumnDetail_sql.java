package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author zengwang
 * @create 2023-09-30 15:52
 * @desc:
 */
public class Demo8_ColumnDetail_sql {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tenv.executeSql(
                "create table t_person                                  "
                        + " (                                                   " // 物理字段
                        + "   id int,                                           "
                        + "   name map<string, string>,                         "
                        + "   age int,                                          "
                        + "   gender string,                                     "
                        + "   guid as id,  "  // 表达式字段，as之后是要有物理字段的声明
                        + "   big_age as age + 10, "
                        + "   offs bigint metadata from 'offset', " // 元数据字段 from之后是元数据的key名称--字符串
                        + "   ts TIMESTAMP_LTZ(3) metadata from 'timestamp'   "
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
        tenv.executeSql("desc t_person").print();
        tenv.executeSql("select * from t_person").print();
    }
}
