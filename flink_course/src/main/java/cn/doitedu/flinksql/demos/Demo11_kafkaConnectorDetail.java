package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2023-10-17 15:21
 * @desc:
 */
public class Demo11_kafkaConnectorDetail {
    public static void main(String[] args) throws Exception {
        /*  更复杂的获取kafka中数据，key、value（同key中字段重名）、headers：
         *      key: {"k1":100,"k2":"hello"}
         *      value: {"guid":1,"eventId":"e02","eventTime":1655017433000,"k1": "ss", "k2": "pp"}
         *      headers:
         *          name ->  "vvvv"
         *          age ->  "30"  // flink暂时不支持把bytes 转成int
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        tenv.executeSql(
                " create table t_events(                                          "
                        + "   guid int,                                                     "
                        + "   eventId string,                                               "
                        + "   eventTime bigint,                                             "
                        + "   k1  string,                                               "
                        + "   k2  string,                                               "
                        // key 部分的参数
                        + "   inKey_k1  int,                                               "
                        + "   inKey_k2  string,                                               "
                        // 连接器的元数据的参数，元数据key和字段名同名则不要额外写 from
                        + "   `offset` bigint metadata," // offset是sql关键字，因此要带``
                        // headers官方只能是map<string, bytes>, 因为header本身就是一个json，json的key要求必须是string
                        + "   headers map<string, bytes> metadata,"

                        // 水位线定义
                        + "   rt as to_timestamp_ltz(eventTime,3),                          "
                        + "   watermark for rt  as rt - interval '1' second                 "
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'kafka',                                        "
                        + "   'topic' = 'doit30-kafka',                                   "
                        + "   'properties.bootstrap.servers' = 'hadoop102:9092',              "
                        + "   'properties.group.id' = 'g1',                                 "
                        + "   'scan.startup.mode' = 'latest-offset',                      "
                        + "   'key.format'='json',                               "
                        + "   'key.json.ignore-parse-errors' = 'true',           "
                        + "   'key.fields'='inKey_k1;inKey_k2',                              "
                        +  "  'key.fields-prefix'='inKey_',                   "
                        + "   'value.format'='json',                             "
                        + "   'value.json.fail-on-missing-field'='false',        "
                        + "   'value.json.ignore-parse-errors' = 'true',        "
                        + "   'value.fields-include' = 'EXCEPT_KEY'              " // 声明将key中的数据纳入解析范畴
                        + " )                                                               "
        );

        tenv.executeSql("desc t_events").print();
         tenv.executeSql("select guid,eventTime,k1,k2," +
                 "inKey_k1, inKey_k2," + // key部分参数
                 // 元数据部分参数
                 "`offset`, cast(headers['name'] as string) as name, cast(headers['age'] as string) as age" +
                 " from t_events").print();
    }
}
