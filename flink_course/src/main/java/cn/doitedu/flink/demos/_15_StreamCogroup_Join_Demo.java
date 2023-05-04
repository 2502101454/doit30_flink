package cn.doitedu.flink.demos;

import com.sun.prism.impl.BaseMesh;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;

/**
 * @author zengwang
 * @create 2023-04-28 17:20
 * @desc:
 */
public class _15_StreamCogroup_Join_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);

        // 输入数字字符串
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        // 输入字母字符串
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        /**
         * 流的 cogroup
         * 案例背景：
         *    流1数据：  id,name
         *    流2数据：  id,age,city
         *    利用coGroup算子，来实现两个流的数据按id相等进行窗口关联（包含inner ，left， right， outer）
         */
        // id, name
        SingleOutputStreamOperator<Tuple2<String, String>> nameStream = stream1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });

        // id, age, city
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> ageCityStream = stream2.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Integer.parseInt(split[1]), split[2]);
            }
        });

        DataStream<String> coGroupStream = nameStream.coGroup(ageCityStream)
                .where(tuple2 -> tuple2.f0) // 左流的key
                .equalTo(tuple3 -> tuple3.f0) // 右流的key
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                // new CoGroupFunction<左流类型, 右流类型, 两个流 协同处理后的输出类型>
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>() {
                    /**
                     * 在窗口结束时，对累积的数据中每个key 调用coGroup方法，计算后输出
                     * !!因此会在窗口结束时产生大量输出，窗口时间内，不做输出!!
                     *
                     * @param first 该key在协同组中，左流的数据，可空
                     * @param second 该key在协同组中，右流的数据，可空 (first、second 不会同时为空，key就是从组内选出来的)
                     * @param out 要输出的数据类型
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first,
                                        Iterable<Tuple3<String, Integer, String>> second,
                                        Collector<String> out) throws Exception {
                        // key并未单独拎出来在参数里
                        // 实现全连接
                        boolean first_flag = false;
                        boolean second_flag = false;
                        for (Tuple2<String, String> fr : first) {
                            first_flag = true;
                            for (Tuple3<String, Integer, String> se : second) {
                                second_flag = true;
                                out.collect(MessageFormat.format("id {0} name {1} age {2} city {3}", fr.f0, fr.f1, se.f1, se.f2));
                            }
                        }
                        // 由于不能全为空，没有左，就一定有右
                        if (!first_flag) {
                            second_flag = true;
                            for (Tuple3<String, Integer, String> se : second) {
                                out.collect(MessageFormat.format("id {0} name {1} age {2} city {3}", se.f0, null, se.f1, se.f2));
                            }
                        }

                        if (!second_flag) {
                            first_flag = true;
                            for (Tuple2<String, String> fr : first) {
                                out.collect(MessageFormat.format("id {0} name {1} age {2} city {3}", fr.f0, fr.f1, null, null));
                            }
                        }

                    }
                });

        // coGroupStream.print();
        // 只能实现inner-join
        DataStream<String> joined = nameStream.join(ageCityStream)
                .where(tuple2 -> tuple2.f0)
                .equalTo(tuple3 -> tuple3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new FlatJoinFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>() {
                    // 窗口结束时，两个流累积的数据，每一对可以join上的，调用此处方法
                    @Override
                    public void join(Tuple2<String, String> first, Tuple3<String, Integer, String> second, Collector<String> out) throws Exception {
                        out.collect(MessageFormat.format("id {0} name {1} age {2} city {3}", first.f0, first.f1,
                                second.f1, second.f2));
                    }
                });
        joined.print();
        env.execute();

    }
}
