package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author zengwang
 * @create 2023-05-27 11:12
 * @desc:
 */
public class _20_Window_Api_Demo2 {
    public static void main(String[] args) {
        // 举例各种窗口API
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

        SingleOutputStreamOperator<EventBean2> beanWM = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean2>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 eventBean2, long recordTimestamp) {
                        return eventBean2.getTimestamp();
                    }
                }));

        /**
         * 一、各种全局窗口开窗API
         */
        // 全局计数滚动窗口
        beanWM.countWindowAll(10); // 每10条数据 一个窗口
        // 全局计数滑动窗口
        beanWM.countWindowAll(10, 2); // 每来2条数据，计算最近10条数据
        // 全局事件时间滚动窗口
        beanWM.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        // 全局事件时间滑动窗口
        beanWM.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // 全局事件时间会话窗口
        beanWM.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30)));
        // 全局处理时间滚动窗口
        beanWM.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 全局处理时间滑动窗口
        beanWM.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // 全局处理时间会话窗口
        beanWM.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


        /**
         * 二、各种Keyed窗口开窗API
         */
        KeyedStream<EventBean2, Long> keyedBean = beanWM.keyBy(EventBean2::getGuid);
        // keyed计数滚动窗口
        keyedBean.countWindow(10);
        // keyed计数滑动窗口
        keyedBean.countWindow(10, 2);
        // keyed事件时间滚动窗口
        keyedBean.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        // keyed事件时间滑动窗口
        keyedBean.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // keyed事件时间会话窗口
        keyedBean.window(EventTimeSessionWindows.withGap(Time.seconds(30)));
        // keyed处理时间滚动窗口
        keyedBean.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // keyed处理时间滑动窗口
        keyedBean.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // keyed处理时间会话窗口
        keyedBean.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));
    }
}
