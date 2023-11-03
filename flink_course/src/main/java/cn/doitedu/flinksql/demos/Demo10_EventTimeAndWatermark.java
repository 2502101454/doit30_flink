package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-17 15:21
 * @desc:
 */
public class Demo10_EventTimeAndWatermark {
    public static void main(String[] args) {
        /*
                watermark 在DDL中的定义示例代码
         *
         *    测试数据 case1：
         *    {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
         *    {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
         *    {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
         *    case 2:
         *    {"guid":1,"eventId":"e05","eventTime":"2020-04-15 20:13:40.564","pageId":"p001"}
         *    {"guid":1,"eventId":"e06","eventTime":"2020-04-15 20:13:41.564","pageId":"p001"}
         *    {"guid":1,"eventId":"e07","eventTime":"2020-04-15 20:13:42.564","pageId":"p001"}
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        // 只有timestamp、timestamp_ltz类型的字段才可以用来设置watermark
        // yyyy-mm-dd hh:MM:ss 建议使用timestamp; epoch 时间戳类型 建议使用timestamp_ltz
        tenv.executeSql(
                "create table t_events                                  "
                        + " (                                                   "
                        + "   guid int,                                           "
                        + "   eventId string,                         "
                        //+ "   eventTime bigint,                                          "
                        + "   eventTime timestamp(3),                                          "
                        + "   pageId string,                                     "
                        + "   pt as proctime(),                                 " // 声明process time属性
                        //+ "   rt as to_timestamp_ltz(eventTime, 3),          " // 代表秒精确到毫秒
                        //+ "   watermark for rt as rt - interval '0.001' second  " // 设置wm 为eventTime减去1毫秒
                        + "   watermark for eventTime as eventTime - interval '1' second"
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'event-log',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

         //tenv.executeSql("desc t_events").print();
        // tenv.executeSql("select guid, eventId, eventTime, pageId, pt, rt, CURRENT_WATERMARK(rt) as wm from t_events")
        tenv.executeSql("select guid, eventId, eventTime, pageId, pt, CURRENT_WATERMARK(eventTime) as wm from t_events")
                .print();
    }
}
