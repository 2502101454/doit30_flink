package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.MessageFormat;
import java.time.Duration;

/**
 * @author zengwang
 * @create 2023-05-27 15:05
 * @desc:
 */
public class _20_Window_Api_Demo3 {
    public static void main(String[] args) throws Exception {
        // 观察水位线 delta、窗口允许延迟、侧流兜底
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<EventBean2> beanStream = s1.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).returns(EventBean2.class);

        SingleOutputStreamOperator<EventBean2> beanWM = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 eventBean2, long recordTimestamp) {
                        return eventBean2.getTimestamp();
                    }
                }));
        OutputTag<EventBean2> outputTag = new OutputTag<EventBean2>("lateData", TypeInformation.of(EventBean2.class));
        /*
        需求：每10s统计最近30s，每个用户的行为次数
        希望：乱序情况突出，希望能尽快输出统计结果，随后适当修正，最后不行再放弃，不能延迟太久
         */
        SingleOutputStreamOperator<String> resultStream = beanWM.keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<EventBean2, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<EventBean2, String, Long, TimeWindow>.Context context,
                                        Iterable<EventBean2> elements, Collector<String> out) throws Exception {
                        long watermark = context.currentWatermark();
                        TimeWindow window = context.window();
                        int i = 0;
                        for (EventBean2 bean : elements) {
                            i += 1;
                        }
                        out.collect(MessageFormat.format("currentWatermark: {0} window [{1}, {2}) agg {3}",
                                watermark, window.getStart(), window.getEnd(), i));
                    }
                });

        DataStream<EventBean2> sideOutput = resultStream.getSideOutput(outputTag);
        resultStream.print("主流输出");
        sideOutput.print("侧流输出");

        env.execute();

    }
}
