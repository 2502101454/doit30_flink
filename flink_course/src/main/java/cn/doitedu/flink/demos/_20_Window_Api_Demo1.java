package cn.doitedu.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.util.*;

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
        // guid, eventId,timestamp, pageId, actTimeLong
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
        /**滚动聚合api使用示例:
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

        /**滚动聚合api使用示例:
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

        /**滚动聚合api使用示例:
         * TODO 补充练习 1
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 使用sum算子来实现, sum输出也是整条数据，同理于max，首条数据(其他字段 + sum字段被修改)
         * 有意思：bean又被套了一层tuple2，window是如何找到bean的timestamp，从而分配到对应的bucket内？
         * 自动拆包? 还是从上游来的bean 都是带上一个flag，bean转换的新数据也会继承flag?
         */
        watermarkedBeanStream.map(bean -> Tuple2.of(bean, 1)).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
                .keyBy(tuple2 -> tuple2.f0.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum("f1")
                .print("sum tuple--->");

        /**滚动聚合api使用示例:
         * TODO 补充练习 2
         * 需求 一：  每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长，整条记录
         * 用max算子来实现(返回 <<首条数据 apply 局部字段的更新>>)
         * 需求二：每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长以及所在的那条行为记录
         * 用maxBy算子 返回最大的那条数据
         */
        // SingleOutputStreamOperator<EventBean2> actLongTime = windowedStream.max("actTimeLong");
        // actLongTime.print();
        // SingleOutputStreamOperator<EventBean2> actLongTime2 = windowedStream.maxBy("actTimeLong");
        // actLongTime2.print();

        /**全量聚合API 使用实例:
         * TODO 补充练习 3
         * 需求二: 每隔10s，统计最近 30s 的数据中，每个用户的行为事件中，行为时长最长的前2条记录
         * 用 apply 算子实现
         */
        // WindowFunction<输入，输出，key，窗口>
        windowedStream.apply(new WindowFunction<EventBean2, EventBean2, Long, TimeWindow>() {
            @Override
            public void apply(Long key, TimeWindow window, Iterable<EventBean2> input, Collector<EventBean2> out) throws Exception {
                // 获取window的元信息
                long maxTimestamp = window.maxTimestamp();
                long start = window.getStart();
                long end = window.getEnd();
                // 每个Key是一个独立维护的窗口，因此2个key的话，这里就执行两次
                // 窗口是在自己的maxTimestamp的时候触发计算的，和endTime无关，比如滑动窗口，每10s，计算最近30s的数据，
                // 有窗口[-10000, 20000)ms 窗口的maxTimestamp是19999, end 是2000, 当算子的wm是19999的时候就会触发窗口计算

                // System.out.println(MessageFormat.format("--apply window: maxTimestamp {0} start {1} end {2}",
                //        maxTimestamp, start, end));
                // --apply window: maxTimestamp 19,999 start -10,000 end 20,000

                ArrayList<EventBean2> beans = new ArrayList<>();
                for (EventBean2 ele : input) {
                    beans.add(ele);
                }
                // low B 写法，可以使用treeMap进行优化
                Collections.sort(beans, new Comparator<EventBean2>() {
                    @Override
                    public int compare(EventBean2 o1, EventBean2 o2) {
                        // 参数顺序 和 使用参数的顺序 不一致，表示逆序
                        return o2.getActTimeLong() - o1.getActTimeLong();
                    }
                });

                // 输出前2个，兼容event id只有一种的情况
                for (int i = 0; i < Math.min(beans.size(), 2); i++) {
                    out.collect(beans.get(i));
                }
            }
        }); //.print();

        /**全量聚合API 使用实例:
         * TODO 补充练习 4
         * 需求 一： 每隔10s，统计最近30s 的数据中，每个页面上发生的行为中，不同事件的平均时长最大的前2种事件及其平均时长
         * 用 process算子来实现，可以得到更多信息，比如上下文
         */
        watermarkedBeanStream.keyBy(EventBean2::getPageId)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                // ProcessWindowFunction<输入的类型，输出的类型Tuple3<pageId, eventId, 时长>，key的类型，timeWindow>
                .process(new ProcessWindowFunction<EventBean2, Tuple3<String, String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<EventBean2, Tuple3<String, String, Double>, String, TimeWindow>.Context context,
                                        Iterable<EventBean2> elements,
                                        Collector<Tuple3<String, String, Double>> out) throws Exception {
                        String taskName = getRuntimeContext().getTaskName();
                        // 从上下文获取更多信息
                        long currentWatermark = context.currentWatermark();
                        TimeWindow window = context.window();
                        // 获取window的元信息
                        long maxTimestamp = window.maxTimestamp();
                        long start = window.getStart();
                        long end = window.getEnd();

                        //System.out.println(MessageFormat.format("--process window: taskName {0} currentWM {1} " +
                        //        "maxTimestamp {2} start {3} end {4}", taskName, currentWatermark, maxTimestamp, start, end));

                        // HashMap<事件id, Tuple2<该事件次数, 该事件的所有时长>>
                        HashMap<String, Tuple2<Integer, Long>> map = new HashMap<>();
                        // 求每种事件 的平均时长
                        for (EventBean2 element : elements) {
                            String eventId = element.getEventId();
                            Tuple2<Integer, Long> countAndTimeLong = map.getOrDefault(eventId, Tuple2.of(0, 0L));
                            countAndTimeLong.setFields(countAndTimeLong.f0 + 1, countAndTimeLong.f1 + element.getActTimeLong());
                            map.put(eventId, countAndTimeLong);
                        }

                        ArrayList<Tuple2<String, Double>> tmpList = new ArrayList<>();
                        for (Map.Entry<String, Tuple2<Integer, Long>> entry : map.entrySet()) {
                            Tuple2<Integer, Long> countAndTimeLong = entry.getValue();
                            tmpList.add(Tuple2.of(entry.getKey(), (double) countAndTimeLong.f1 / countAndTimeLong.f0));
                        }
                        // 通过排序求top2
                        Collections.sort(tmpList, new Comparator<Tuple2<String, Double>>() {
                            @Override
                            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                                /**根据参数顺序和使用比较句柄的顺序是否一致，来理解排序顺序
                                 * 1.正序：o1, o2；o1.compareTo(o2)， 顺序一致
                                 * 2.逆序: o1, o2；o2.compareTo(o1), 顺序相反
                                 */
                                // return o1.f1.compareTo(o2.f1);
                                return Double.compare(o2.f1, o1.f1);
                            }
                        });

                        // 输出前2个，兼容event id只有一种的情况
                        for (int i = 0; i < Math.min(tmpList.size(), 2); i++) {
                            out.collect(Tuple3.of(key, tmpList.get(i).f0, tmpList.get(i).f1));
                        }
                    }
                });//.print();



        // 什么时候触发窗口? [0, 10s) [0, 9999]ms 都是窗口要的，窗口在9999也不能触发，是窗口范围的闭区间呀，必须等到10000ms?
        // 每条数据到来了，计算该条数据对应的窗口的起始位置：
        // 第一个窗口 start = 1970-01-01 00:00:00 - ((当前数据时间 - 1970-01-01 00:00:00) % 窗口长度)
        // 第一个窗口 end = start + 窗口长度
        // 第二个窗口 start = 第一个窗口end， 以此类推
        // 滚动聚合，给下游最终只能输出一个值，如果要求top2，还得拼接一起输出;
        // 全量聚合 输出由你自己指定，个数类型不限制
        env.execute();
    }
}
