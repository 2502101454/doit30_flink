package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.MessageFormat;

/**
 * @author zengwang
 * @create 2023-05-03 14:36
 * @desc:
 */
public class _17_ProcessFunctions_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);
        // id, event
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        // id, age, city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        /**
         * 在普通的datastream 上调用process算子，传入"ProcessFunction"
         */
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            // 可使用生命周期open方法
            @Override
            public void open(Configuration parameters) throws Exception {
                // 可以调用 getRuntimeContext 方法拿到各种运行时上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();
                super.open(parameters);
            }

            // 可使用生命周期close方法
            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                String[] split = value.split(",");
                Tuple2<String, String> tuple2 = Tuple2.of(split[0], split[1]);
                // 可做侧流输出
                ctx.output(new OutputTag<Tuple2<String, String>>("s1_slide", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                })), tuple2);
                // 可做主流输出
                out.collect(tuple2);
            }
        });

        /**
         * 在keyedStream上调用process算子, 这里没有进行聚合，而是做了map
         */
        SingleOutputStreamOperator<Tuple2<Integer, String>> keyedProcess = s1.keyBy(tuple2 -> tuple2.f0)
                // KeyedProcessFunction<Key的类型，处理的数据类型，输出的类型>
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
                    @Override
                    public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String,
                            Tuple2<String, String>, Tuple2<Integer, String>>.Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                        // 把Id变整数,eventId变大写
                        int id = Integer.parseInt(value.f0);
                        String event = value.f1.toUpperCase();
                        out.collect(Tuple2.of(id, event));
                    }
                });

        keyedProcess.print();

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> userInfoStream = stream2.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Integer.parseInt(split[1]), split[2]);
            }
        });

        env.execute();

    }
}
