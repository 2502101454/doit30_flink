package cn.doitedu.flink.demos;

/**
 * @author zengwang
 * @create 2023-03-06 23:26
 * @desc:
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***
 * 需求：得到socket数据流，然后统计数据流中出现的单词及其个数，要求输出累积至今的结果
 */
public class _01_StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建一个编程入口环境
        // ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); // 批处理入口环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 流批一体入口环境

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        // 显式声明为本地运行环境，并开启webUIU (getExecutionEnvironment内部会自动判断是本地还是集群); 默认端口是随机的，我们指定
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        /**
         * 本地也是多线程运行，默认使用cpu的逻辑核数；
         * 使用流计算处理流数据；
         */
//        env.setParallelism(2);

        // 通过source算子，把socket数据源 加载为一个dataStream数据流
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 对source数据流进行转换(计算逻辑)，得到新的流
        // 使用父类来引用子类对象，因为子类名实在太长了
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 切割单词
                String[] split = s.split("\\s+");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 父类还是DataStream
        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyed.sum("f1");
        // 通过sink算子(print是算子)，将结果输出 wcSink:1> xxxxx， 1代表算子子任务的id，从1开始
        DataStreamSink<Tuple2<String, Integer>> wcSink = resultStream.print("wcSink");

        // 触发程序的提交运行
        env.execute();
    }
}
