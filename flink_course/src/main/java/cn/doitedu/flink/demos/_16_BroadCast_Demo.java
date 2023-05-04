package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.util.Locale;

/**
 * @author zengwang
 * @create 2023-05-03 14:36
 * @desc:
 */
public class _16_BroadCast_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        // id, event
        SingleOutputStreamOperator<Tuple2<String, String>> actStream = stream1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });

        // id, age, city
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> userInfoStream = stream2.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Integer.parseInt(split[1]), split[2]);
            }
        });
        /**
         * 案例背景：
         *    用户行为流，数据持续不断：  id,event
         *    用户维度流，数据稀疏：  id,age,city
         *
         * 需要用户维度进行填充到行为流中
         * 底层逻辑：
         *  1.将维度流转为广播流
         *  2.利用connect算子，对两个流进行绑定一起，可以共享算子状态： 广播流更新状态，主流读取状态
         */

        // 状态描述器
        MapStateDescriptor<String, Tuple2<Integer, String>> userInfoStateDescription = new MapStateDescriptor<>(
                "userInfoStateDescription", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));
        // 把用户维度所在的流 转成广播流，广播流数据将做为后边process算子的状态数据
        BroadcastStream<Tuple3<String, Integer, String>> broadcastStream = userInfoStream.broadcast(userInfoStateDescription);

        // 哪个流需要用到广播状态数据，就要来 connect这个广播流
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, Integer, String>> connect = actStream.connect(broadcastStream);
        SingleOutputStreamOperator<String> process = connect.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>() {
            /**
             * 本方法 是处理主流中的数据(每来一条，调用一次)
             * @param value 左流(主流)的一条数据
             * @param ctx 上下文
             * @param out 输出器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 从上下文获取 "只读"的状态对象
                ReadOnlyBroadcastState<String, Tuple2<Integer, String>> broadcastState = ctx.getBroadcastState(userInfoStateDescription);
                Tuple2<Integer, String> userInfo = broadcastState.get(value.f0);
                out.collect(MessageFormat.format("{0} {1} {2} {3}", value.f0, value.f1,
                        userInfo == null ? null : userInfo.f0, userInfo == null ? null : userInfo.f1));
            }

            /** 哪个流需要广播流数据了，就去connect广播流，广播流就会再后边的主流算子中，发给所有subTask的状态
             * 本方法 是处理广播流中的数据(每来一条，调用一次)
             * @param value 广播流的一条数据
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, Integer, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, Integer, String>, String>.Context ctx, Collector<String> out) throws Exception {
                // 从上下文中获取广播状态对象(可读可写的状态对象)
                BroadcastState<String, Tuple2<Integer, String>> broadcastState = ctx.getBroadcastState(userInfoStateDescription);
                // 解析广播流数据，装入广播状态
                broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
            }
        });

        process.print();

        env.execute();

    }
}
