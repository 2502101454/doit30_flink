package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author zengwang
 * @create 2023-11-26 15:47
 * @desc:
 */
public class Demo25_CustomTableAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置算子的并行度，算子的一个运行实例就是1个线程
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1, "male", 88),
                Row.of(2, "male", 78),
                Row.of(3, "male", 80),
                Row.of(4, "female", 90),
                Row.of(5, "female", 85),
                Row.of(6, "female", 95)
        );
        tenv.createTemporaryView("t", table);
        // 注册自定义函数, 实现各个性别中的top2的记录, 这里没有行号，
        tenv.createTemporarySystemFunction("top2", Top2.class);
        // 暂时只支持DSL语法调用
        tenv.from("t")
                .groupBy($("gender"))
                .flatAggregate(call("top2", row($("id"), $("gender"),  $("score"))))
                // group by gender，以及表聚合函数返回的row中gender，命名冲突了，要指定求哪个，因此下面在生成的表命名为rgender
                //.select($("id"), $("gender"), $("score"))
                .select($("id"), $("rgender"), $("score"))
                .execute().print();
    }

    public static class Top2Accumulator {
        public @DataTypeHint("ROW<id INT,gender STRING,score DOUBLE>") Row first;
        public @DataTypeHint("ROW<id INT,gender STRING,score DOUBLE>") Row second;
    }

    @FunctionHint(input =@DataTypeHint("ROW<id INT,gender STRING,score DOUBLE>") ,output = @DataTypeHint("ROW<id INT, rgender STRING,score DOUBLE>"))
    public static class Top2 extends TableAggregateFunction<Row, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Row.of(null, null, Double.MIN_VALUE);
            acc.second = Row.of(null, null, Double.MIN_VALUE);
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Row value) {
            double score = (double) value.getField("score");
            if (score > (double) acc.first.getField("score")) {
                acc.second = acc.first;
                acc.first = value;
            } else if (score > (double) acc.second.getField("score")) {
                acc.second = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Row> out) {
            // emit the value and rank
            if ((double)acc.first.getField("score") != Double.MIN_VALUE) {
                out.collect(acc.first); // 可以再次重更新构造row，带上行号
            }
            if ((double)acc.second.getField("score") != Double.MIN_VALUE) {
                out.collect(acc.second);
            }
        }
    }

}
