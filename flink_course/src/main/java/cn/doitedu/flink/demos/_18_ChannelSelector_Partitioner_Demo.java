package cn.doitedu.flink.demos;

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author zengwang
 * @create 2023-05-07 15:56
 * @desc:
 */
public class _18_ChannelSelector_Partitioner_Demo {
    public static void main(String[] args) throws Exception {
        // wordCount Demo 练习并行度、算子链、传输规则API
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // env.setParallelism(2);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> line = streamSource.map(String::toUpperCase);
        DataStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                Arrays.stream(value.split("\\s+")).forEach(out::collect);
            }
        }).rebalance();
        //.rebalance(); // 上下都16，改上游subTask数据传输方式为rebalance，则不会合并算子链
        //.shuffle(); // 上游subTask的数据传输变成shuffle
        //.broadcast(); // 上下游并行度都是16，上游subTask的数据传输变成broadcast
        //.startNewChain(); // 上下游并行度都是16，上游subTask的数据传输变成forward
        //.slotSharingGroup("wz_group"); // slot不够用，程序就没法跑咯

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2 = words.map(s -> Tuple2.of(s, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})).setParallelism(16);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedSum = keyedStream.sum("f1");
        keyedSum.print().setParallelism(8);

        env.execute();
    }
}
