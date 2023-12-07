package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author zengwang
 * @create 2023-11-26 15:47
 * @desc:
 */
public class Demo22_CustomScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置算子的并行度，算子的一个运行实例就是1个线程

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        /*Table table = tenv.fromValues(
                DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())),
                Row.of("aaa"),
                Row.of("bb"),
                Row.of("cc")
        );*/
        // 可能是flink版本问题，上面的table.fromValues创建方式会报Minicluster状态异常
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9998);
        tenv.createTemporaryView("t", s1);
        // 注册自定义函数
        tenv.createTemporarySystemFunction("my_upper", MyUpper.class);
        // 注册之后即可在SQL中使用了
        tenv.executeSql("select my_upper(f0) from t\n").print();

    }

    public static class MyUpper extends ScalarFunction {
        public String eval(String line) {
            return line.toUpperCase();
        }
    }
}
