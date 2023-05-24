package cn.doitedu.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.time.Duration;

/**
 * @author zengwang
 * @create 2023-05-07 15:56
 * @desc:
 */
public class _19_WaterMark_API_Demo2 {
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
        env.getConfig().setAutoWatermarkInterval(10000);
        // 1,e01,16827393902,page01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        /*
         *从中间环节的某个算子开始生成watermark(注意: 如果源头上已经生成了watermark，就不要在下游再生成了)
         */
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

        SingleOutputStreamOperator<EventBean> s2 = map.assignTimestampsAndWatermarks(watermarkStrategy2).setParallelism(2);
        // 观察watermark
        // 算子A，发射更新后的水位线，其总是插在触发A的wm更新的 元素的身后
        // 1.在流中，更新后的水位线总是插在促使其更新的元素的身后
        // 2.数据先来取的总是老的wm，等后边新的wm来了后，求一次min(all_partitions)，然后更新算子的wm，下一轮数据再来，才可以读到这个更新的wm

        SingleOutputStreamOperator<EventBean> process = s2.process(new ProcessFunction<EventBean, EventBean>() {
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

        process.print();
        env.execute();
    }
}
