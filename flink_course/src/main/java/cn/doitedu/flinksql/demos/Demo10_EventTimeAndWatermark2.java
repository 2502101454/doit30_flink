package cn.doitedu.flinksql.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2023-10-17 15:21
 * @desc:
 */
public class Demo10_EventTimeAndWatermark2 {
    public static void main(String[] args) throws Exception {
        /*
         *    {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
         *    {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        // datastreamAPI 中wm总会减去1毫秒
        SingleOutputStreamOperator<Event> s2 = s1.map(line -> JSON.parseObject(line, Event.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps() // 总是取数据的最大时间戳，不会减去delta，因此不允许乱序
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );
        // 观察datastream API中的水位线
        s2.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                TimerService timerService = ctx.timerService();
                out.collect(value + "=>" + timerService.currentWatermark());
            }
        }).print();

        // 把 流 转成 表，会丢失watermark
        tenv.createTemporaryView("t_events", s2);
        // SQL表上的字段类型是基于Event class中的字段的，没有一个timestamp or timestamp_ltz的字段，下面的sql也执行不了呀
        // tenv.executeSql("select guid, eventId, eventTime, pageId, CURRENT_WATERMARK(eventTime) as wm from t_events").print();

        // 把表再转回流，来验证确实丢失了watermark
        /*DataStream<Row> t_events = tenv.toDataStream(tenv.from("t_events"));
        t_events.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(row + "" + ctx.timerService().currentWatermark());
            }
        }).print();*/

        // 解决方案: 在流转成表时，用户显式的声明watermark策略
        tenv.createTemporaryView("t_events2", s2, Schema.newBuilder()
                        .column("guid", DataTypes.INT())
                        .column("eventId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .column("pageId", DataTypes.STRING())

                        // 生成watermark的条件，需要timestamp、timestamp_ltz类型的字段，这里给造出来
                        // 方式1，用数据自身的字段转出来一个timestamp类型的字段
                        //.columnByExpression("rt", "to_timestamp_ltz(eventTime, 3)")
                        // 方式2, 使用流转表时，底层的连接器的元数据，此元数据为rowtime 取值为watermark策略抽取的值 >> extractTimestamp
                        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")

                        // 自定义watermark 的策略，两种写法
                        // 方式1. 延迟自己写，比如写10s，和之前datastreamAPI中的延迟策略不一样
                        .watermark("rt", "rt - interval '10' seconds")
                        // 方式2. 直接和之前datastream API设的watermark算法保持一致，懒得自己再写一遍
                        //.watermark("rt", "source_watermark()")
                .build());

        tenv.executeSql("select guid, eventId, eventTime, pageId, rt, CURRENT_WATERMARK(rt) as wm from t_events2").print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        public int guid;
        public String eventId;
        public long eventTime;
        public String pageId;
    }
}
