package cn.doitedu.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;

/**
 * @author zengwang
 * @create 2023-05-07 15:56
 * @desc:
 *
 *  测试数据 ：
 *  1,e01,10000,p01,10
 *  1,e02,11000,p02,20
 *  1,e02,12000,p03,40
 *  1,e03,20000,p02,10
 *  1,e01,21000,p03,50
 *  1,e04,22000,p04,10
 *  1,e06,28000,p05,60
 *  1,e07,30000,p02,10
 */
public class _20_Window_Api_Demo1 {
    public static void main(String[] args) throws Exception {
        // 多并行度下的watermark 观察
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
         env.setParallelism(1);
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // env.getConfig().setAutoWatermarkInterval(10000);
        // guid, event, pageId, actTimeLong
        // 1,e01,10000,page01,10
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<EventBean2> beanStream = s1.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).returns(EventBean2.class);

        WatermarkStrategy<EventBean2> watermarkStrategy = WatermarkStrategy
                .<EventBean2>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                });

        // 分配wm，以推进时间
        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(watermarkStrategy);
        // WindowedStream<数据类型, key类型, window类型>
        WindowedStream<EventBean2, Long, TimeWindow> windowedStream = watermarkedBeanStream.keyBy(EventBean2::getGuid)
                // window(窗口长度，滑动步长)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));
        // window算子 对每个key的stream进行开窗，得到一个开窗了的Stream
        /**
         * 滚动聚合 API 使用示例
         * 需求 一： 每隔10s，统计最近30s数据中，每个用户的行为条数
         * 使用aggregate算子实现：
         *  算子内部维护一个状态变量——累加器，里面存储我们需要的信息
         */
        // 对开窗了的stream的每个窗口进行聚合
        SingleOutputStreamOperator<Integer> resultStream = windowedStream
                // reduce: 滚动聚合算子，有限制：聚合结果必须和数据类型是一致
                /*.reduce(new ReduceFunction<EventBean>() {
                    @Override
                    public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                        return null;
                    }
                })*/
                // AggregateFunction<输入的数据类型, 累加器类型, 累加器输出结果的类型>
                .aggregate(new AggregateFunction<EventBean2, Integer, Integer>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 滚动聚合的逻辑(拿到一条数据，如何去更新累加器)
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer add(EventBean2 value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    /**
                     * 基于累加器中，计算出窗口要输出的结果
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    /**
                     * 流计算模式，不用实现!
                     *
                     * 批计算模式下，在上游预聚合的场景，比如Spark的reduceByKey，上游的局部聚合值放在累加器中
                     * 下游对收到的累加器进行merge，这里和合并逻辑
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        // resultStream.print(">");

        /**
         * 需求二：每10s，统计最近30s数据中，每个用户的平均行为时长
         * 注意：使用滚动聚合，累加器定义, Tuple2<行为次数, 行为总时长>
         */
        SingleOutputStreamOperator<Double> resultStream2 = windowedStream.aggregate(new AggregateFunction<EventBean2, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return Tuple2.of(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(EventBean2 value, Tuple2<Integer, Integer> accumulator) {
                accumulator.setFields(accumulator.f0 + 1, accumulator.f1 + value.getActTimeLong());
                return accumulator;
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                return accumulator.f1 / (double) accumulator.f0;
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
            }
        });
        // resultStream2.print()

        // 什么时候触发窗口? [0, 10s) [0, 9999]ms 都是窗口要的，窗口在9999也不能触发，是窗口范围的闭区间呀，必须等到10000ms?

        // 滚动聚合，给下游最终只能输出一个值，如果要求top2，还得拼接一起输出;
        // 全量聚合 输出由你自己指定，个数类型不限制
        env.execute();
    }
}
