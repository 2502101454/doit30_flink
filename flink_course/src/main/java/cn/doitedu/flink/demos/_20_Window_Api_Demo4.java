package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.util.Iterator;

/**
 * @author zengwang
 * @create 2023-05-29 16:28
 * @desc:
 */
public class _20_Window_Api_Demo4 {
    public static void main(String[] args) throws Exception {
        /**
         * 需求：练习触发器和移除器的使用，对于10s的滚动窗口
         * 1.除了正常到达窗口结束时触发计算，还要能够在eventId是wz的时候进行一次触发
         *
         * 2.每次触发计算时，不要包含其中wz这条数据
         * evictor, 在触发窗口计算的时候，可以控制移除哪些数据
         */
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

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
        watermarkedBeanStream.keyBy(EventBean2::getGuid)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyEventTimeTrigger())
                .evictor(MyTimeEvictor.of(Time.seconds(10))) // 滚动窗口，首个窗口的start基于到来的元素timestamp
                // 统计窗口内部的数据条数
                .apply(new WindowFunction<EventBean2, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<EventBean2> input, Collector<String> out) throws Exception {
                        int count = 0;
                        for (EventBean2 eventBean2 : input) {
                            count ++;
                        }
                        out.collect(MessageFormat.format("window [{0}, {1}) count {2}", window.getStart(),
                        window.getEnd(), count));
                    }
                })
                .print();

        env.execute();
    }
}

class MyEventTimeTrigger extends Trigger<EventBean2, TimeWindow> {
    /**
     * 来一条数据时，检查watermark是否超过了窗口的结束点，需要触发窗口计算?
     */
    @Override
    public TriggerResult onElement(EventBean2 element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            // 注册定时器，定时器触发时间为: 窗口结束点时间
            ctx.registerEventTimeTimer(window.maxTimestamp());

            // 判断如果event ID是 wz，则触发一次
            if ("wz".equals(element.getEventId())) return TriggerResult.FIRE;
            return TriggerResult.CONTINUE;
        }
    }

    /**
     * 当处理时间定时器的触发时间（窗口的结束点时间）到达了，检查是否满足触发条件
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * 当事件时间定时器的触发时间（窗口的结束点时间）到达了，检查是否满足触发条件
     * 下面的方法，是定时器在调用
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // 这个time传的是定时器到点的时间
        return time == window.maxTimestamp() ? TriggerResult.FIRE: TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}

class MyTimeEvictor implements Evictor<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long windowSize;
    private final boolean doEvictAfter;

    public MyTimeEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyTimeEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    /**
     * 窗口触发前，调用
     */
    @Override
    public void evictBefore(
            Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**
     * 窗口触发后，调用
     */
    @Override
    public void evictAfter(
            Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**
     * 元素移除的核心逻辑
     */
    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (!hasTimestamp(elements)) {
            return;
        }

        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();

            EventBean2 bean = (EventBean2) record.getValue();

            // 加了一个条件： 数据的eventId=wz，也移除
            if (record.getTimestamp() <= evictCutoff  || bean.getEventId().equals("wz")) {
                iterator.remove();
            }
        }
    }

    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
        Iterator<TimestampedValue<Object>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /**
     * 用于计算移除的时间截止点，debug发现传入的Elements是不包含未来窗口的数据的
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }


    public static MyTimeEvictor of(Time windowSize) {
        return new MyTimeEvictor(windowSize.toMilliseconds());
    }
}
