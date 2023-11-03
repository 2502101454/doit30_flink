package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.TimerService;
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
public class Demo10_EventTimeAndWatermark3 {
    public static void main(String[] args) throws Exception {
        /*    表 转 流，流会携带之前表定义的水位线
         *    {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
         *    {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        tenv.executeSql(
                " create table t_events(                                          "
                        + "   guid int,                                                     "
                        + "   eventId string,                                               "
                        + "   eventTime bigint,                                             "
                        + "   pageId  string,                                               "
                        /*+ "   pt AS proctime(),                                             "*/  // 利用一个表达式字段，来声明 processing time属性
                        + "   rt as to_timestamp_ltz(eventTime,3),                          "
                        + "   watermark for rt  as rt - interval '1' second                 "  // 用watermark for xxx，来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成 eventTime属性及指定watermark策略
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'kafka',                                        "
                        + "   'topic' = 'event-log2',                                   "
                        + "   'properties.bootstrap.servers' = 'hadoop102:9092',              "
                        + "   'properties.group.id' = 'g1',                                 "
                        + "   'scan.startup.mode' = 'latest-offset',                      "
                        + "   'format' = 'json',                                            "
                        + "   'json.fail-on-missing-field' = 'false',                       "
                        + "   'json.ignore-parse-errors' = 'true'                           "
                        + " )                                                               "
        );

        // tenv.executeSql("select guid,eventId,rt,current_watermark(rt) as wm from t_events").print();

        // 实验证明：覆盖掉流默认的水位线机制(减去1毫秒)，而是取表定义的水位线策略
        DataStream<Row> ds = tenv.toDataStream(tenv.from("t_events"));

        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value + " => " + ctx.timerService().currentWatermark());
            }
        }).print();
        env.execute();
    }
}
