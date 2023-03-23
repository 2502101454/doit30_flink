package cn.doitedu.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2023-03-08 20:02
 * @desc:
 */
public class _03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH); // 按批计算模式去执行
        // streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING); // 按流计算模式去执行
        // streamEnv.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); // flink自己去根据数据源决定，默认模式
        streamEnv.setParallelism(1);



        DataStreamSource<String> streamSource = streamEnv.readTextFile("flink_course/src/data/wc/input/wc.txt");
        streamSource.flatMap(new MyFlatMapFunction())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).sum("f1")
                .print();

        streamEnv.execute();
    }
}
