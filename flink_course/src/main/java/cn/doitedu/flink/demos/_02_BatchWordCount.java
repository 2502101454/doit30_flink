package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2023-03-08 13:35
 * @desc:
 */
public class _02_BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        // 读数据：批计算中得到的数据抽象，是一个DataSet
        DataSource<String> stringDataSource = batchEnv.readTextFile("flink_course/src/data/wc/input/wc.txt");

//        FlatMapOperator<String, Tuple2<String, Integer>> wordOne = stringDataSource.flatMap(new MyFlatMapFunction());
        FlatMapOperator<String, Tuple2<String, Integer>> wordOne = stringDataSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                getRuntimeContext().state
            }
        });
        wordOne.groupBy("f0")
                .sum(1)
                .print();
    }
}

class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split("\\s+");
        for (String word : words) {
            out.collect(Tuple2.of(word, 1));
        }

    }
}