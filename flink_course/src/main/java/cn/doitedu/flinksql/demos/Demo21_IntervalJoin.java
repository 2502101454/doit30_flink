package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-11-26 15:47
 * @desc:
 */
public class Demo21_IntervalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置算子的并行度，算子的一个运行实例就是1个线程

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Configuration configuration = tenv.getConfig().getConfiguration();

        /** user_id, ad_item, ts
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         */
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<Tuple3<String, String,Long>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1],Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String,Long>>() {
        });

        /** user_id, play_item, ts
         * 1,v1,1000
         * 2,v2,2000
         */
        DataStreamSource<String> s2 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple3<String, String,Long>> ss2 = s2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1],Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String,Long>>() {
        });

        // 创建两个表，需要有相同类型的time attribute
        tenv.createTemporaryView("t_left",ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '1' second")
                .build());

        tenv.createTemporaryView("t_right",ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '1' second")
                .build());

        /**
         * interval Join
         * 背景: 先观看广告，最晚5s后就会有播放行为
         * 请把这两个流中同一人的连续行为关联输出  (对于播放流而言，一条播放行为在最近的5s内肯定能找到 广告行为)
         */
        tenv.executeSql(
            "select\n" +
                        "a.f0,a.f1,a.f2, b.f0,b.f1,b.f2 \n" +
                        "from t_left as a join t_right as b \n" +
                        "on a.f0 = b.f0 and a.rt between b.rt - interval '5' seconds and b.rt\n"
        ).print();

    }
}
