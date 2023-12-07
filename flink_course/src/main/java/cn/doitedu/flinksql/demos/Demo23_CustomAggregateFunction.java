package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zengwang
 * @create 2023-11-26 15:47
 * @desc:
 */
public class Demo23_CustomAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置算子的并行度，算子的一个运行实例就是1个线程
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9998);
        // id, gender, age
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss1 = s1
        .map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
        })
        .returns(new TypeHint<Tuple3<String, String, Long>>() { });

        tenv.createTemporaryView("t", ss1);
        // 注册自定义函数
        tenv.createTemporarySystemFunction("my_avg", MyAvg.class);
        // 注册之后即可在SQL中使用了
        tenv.executeSql("select f1, my_avg(f2) from t group by f1\n").print();

    }

    public static class MyAccumulator {
        public long count = 0;
        public double sum = 0;
    }

    public static class MyAvg extends AggregateFunction<Double, MyAccumulator> {
        @Override
        public Double getValue(MyAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }

        @Override
        public MyAccumulator createAccumulator() {
            return new MyAccumulator();
        }

        // the accumulate(...) method of the function is called for each input row to update the accumulator
        public void accumulate(MyAccumulator accumulator, Double value) {
            accumulator.count++;
            accumulator.sum += value;
        }
    }
}
