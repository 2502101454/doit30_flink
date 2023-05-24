package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;

/**
 * @author zengwang
 * @create 2023-05-07 15:56
 * @desc:
 */
public class _19_WaterMark_API_Demo3_WZ_MoreChannels {
    public static void main(String[] args) throws Exception {
        // 多并行度下的watermark 观察
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        /**
         * 默认200ms，源头watermark生成的周期，要跟数据的疏密度匹配
         * 实验10000：interval过长，则数据会累积在未来桶，水位线跟不上数据，周期性的延迟计算
         */
        env.getConfig().setAutoWatermarkInterval(500);
        // 1,e01,16827393902,page01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean> map = s1.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        }).returns(EventBean.class);

        WatermarkStrategy<EventBean> watermarkStrategy2 = WatermarkStrategy
                .<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                });

        SingleOutputStreamOperator<EventBean> wm = map.assignTimestampsAndWatermarks(watermarkStrategy2).setParallelism(2);
        // 观察watermark
        // wm算子两个并行度，下游只有一个，因此下游建立两个channel，观察如何更新下游算子的wm?
        // 源码结论：1 v 1是特殊的1 v m，也会走 1 v m 的逻辑
        SingleOutputStreamOperator<EventBean> process = wm.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                // 当前的process time
                long currentTimeMillis = System.currentTimeMillis();
                System.out.println(MessageFormat.format("currentWatermark {0} currentTimeMillis {1} bean {2}",
                        currentWatermark, currentTimeMillis, value));
                out.collect(value);
            }
        });

        // process 发一次wm到print算子
        // print 作为尾部算子还会再发一次wm
        process.print();
        env.execute();
    }
}
