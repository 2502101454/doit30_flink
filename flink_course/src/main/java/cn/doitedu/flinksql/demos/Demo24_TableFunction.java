package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author zengwang
 * @create 2023-11-26 15:47
 * @desc:
 */
public class Demo24_TableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置算子的并行度，算子的一个运行实例就是1个线程
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /*Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("phone_nums", DataTypes.ARRAY(DataTypes.STRING()))
                ),
                Row.of(1, "zs", Expressions.array("138", "137", "139")),
                Row.of(2, "ls", Expressions.array("135", "132", "139"))
        );
        // mini cluster的警告先忽略
        tenv.createTemporaryView("t", table);
        tenv.executeSql("select t.id, t.name, t2.phone_num from t cross join unnest(phone_nums) as t2 (phone_num)")
                .print();*/

        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("phone_nums", DataTypes.STRING())
                ),
                Row.of(1, "zs", "13544,1363,155"),
                Row.of(2, "ls", "135,13263,1393456"),
                Row.of(3, "ww", "") // 注意，该该条数据炸裂的时候：
                // 当为inner join，因为collect 不到东西，所以没结果; left join则保留左表，炸裂字段补null
        );
        tenv.createTemporaryView("t", table);
        // 注册自定义函数, 需求： 实现对phone_nums字段进行切分，炸裂得到每个人的电话号码，以及该号码长度
        tenv.createTemporarySystemFunction("my_split", SplitFunction.class);
        // 这里table1, table2本身就是inner join的写法
        //tenv.executeSql("select id, name, phone, num from t, lateral table(my_split(phone_nums)) \n").print();
        // 重命名炸裂的字段 写法
        //tenv.executeSql("select id, name, pn, n from t, lateral table(my_split(phone_nums)) as t2(pn, n) \n").print();
        // 也可join 关键字写发出来，但是必须添加必为true的条件，1=1也行
        tenv.executeSql("select id, name, pn, n from t join lateral table(my_split(phone_nums)) as t2(pn, n) on True \n").print();
        tenv.executeSql("select * from t left join lateral table(my_split(phone_nums)) on True \n").print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<phone STRING, num INT>")) // 默认这里是虚表的字段名称
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String phone_nums) {
            if (phone_nums == null || phone_nums.length() == 0) {
                return;
            }
            String[] phone_num_arr = phone_nums.split(",");
            for (String phone_num : phone_num_arr) {
                // 调用collect方法将数据emit 一次 就是一行
                collect(Row.of(phone_num, phone_num.length()));
            }
        }
    }
}
