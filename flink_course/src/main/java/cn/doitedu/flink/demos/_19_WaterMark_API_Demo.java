package cn.doitedu.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Arrays;

/**
 * @author zengwang
 * @create 2023-05-07 15:56
 * @desc:
 */
public class _19_WaterMark_API_Demo {
    public static void main(String[] args) throws Exception {
        // 单并行度下的watermark 观察
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        // env.getConfig().setAutoWatermarkInterval(1000 * 10);
        // 1,e01,16827393902,page01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);


        // 如下四种watermark生成策略
        // WatermarkStrategy.noWatermarks(); 不生成watermark，禁用了事件时间推进机制，使用处理时间语义
        // WatermarkStrategy.forMonotonousTimestamps(); 紧跟最大事件时间
        // WatermarkStrategy.forBoundedOutOfOrderness(); 允许乱序的watermark生成策略
        // WatermarkStrategy.forGenerator();  自定义watermark生成策略

        // 创建watermark的生成策略(算法策略、事件时间抽取)
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    /**
                     * 从数据中抽取时间戳
                     * @param element
                     * @param recordTimestamp 此参数暂时无用
                     * @return
                     */
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[2]);
                    }
                });

        /*
         * watermark可以选择在Flow中的任何一个环节生成，整个Flow中只能设置一次
         * 1.从source算子上生成
         * s1.assignTimestampsAndWatermarks(strategy);
         */

        /*
         *从中间环节的某个算子开始生成watermark(注意: 如果源头上已经生成了watermark，就不要在下游再生成了)
         */
        SingleOutputStreamOperator<EventBean> map = s1.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        }).returns(EventBean.class).startNewChain();

        WatermarkStrategy<EventBean> watermarkStrategy2 = WatermarkStrategy
                .<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                });

        SingleOutputStreamOperator<EventBean> s2 = map.assignTimestampsAndWatermarks(watermarkStrategy2);
        // 观察watermark
        // 算子A，发射更新后的水位线，其总是插在触发A的wm更新的 元素的身后
        // 在流中，更新后的水位线总是插在促使其更新的元素的身后
        // 数据流中，水位线总是跟在数据之后
        SingleOutputStreamOperator<EventBean> process = s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {
                // Thread.sleep(5000);
                // 算子时间就是算子的wm
                long currentWatermark = ctx.timerService().currentWatermark();
                // 当前的process time
                long currentTimeMillis = System.currentTimeMillis();
                System.out.println(MessageFormat.format("process 算子：currentWatermark {0} currentTimeMillis {1} bean {2}",
                        currentWatermark, currentTimeMillis, value));
                out.collect(value);
            }
        }).disableChaining();

        process.print();
        env.execute();
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class EventBean {
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;
}