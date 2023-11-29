package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo17_TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 关于时间的三种窗口测试，使用官方提供的样例数据
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);
        // 下单时间、价格、商品
        //2020-04-15 08:05:10,4.00,C
        SingleOutputStreamOperator<Bid> s2 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bid(arr[0], Double.parseDouble(arr[1]), arr[2]);
        });
        // 流转表, 指定schema的水位线
        tenv.createTemporaryView("t_bid", s2,
                Schema.newBuilder()
                        // .column("bidtime", DataTypes.TIMESTAMP()) 官方bug，自己都不支持
                        .column("bidtime", DataTypes.STRING())
                        // 自己加个逻辑字段
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .columnByExpression("rt", "to_timestamp(bidtime)")
                        .watermark("rt", "rt - interval '1' second")
                        // bug: rt字段不连一起写，watermark在表结构中都不能desc show出来
                        .build());

        // tenv.executeSql("desc t_bid").print();
        // tenv.executeSql("select bidtime, rt, current_watermark(rt), price, item from t_bid").print();

        /**
         * 滚动窗口demo: 统计每5min的交易额
         * TUMBLE(TABLE your_table, DESCRIPTOR(time_attr), size [, offset ])
         * 1.14 版本flink不支持单独查看windows table-valued function的结果，必须带aggregate
         */
        String sql = "select window_start, window_end, window_time, sum(price) as sum_price \n" +
                " from table(TUMBLE(table t_bid, descriptor(rt), interval '5' minutes)) \n" +
                " group by window_start, window_end, window_time";

        /**
         * 滑动窗口demo: 每5分钟统计最近10min的交易金额
         * HOP(TABLE data, DESCRIPTOR(time_attr), slide, size [, offset ])
         */
        sql = "select window_start, window_end, window_time, sum(price) as sum_price \n" +
                " from table(HOP(table t_bid, descriptor(rt), interval '5' minutes, interval '10' minutes)) \n" +
                " group by window_start, window_end, window_time";

        /**
         * 累加窗口demo：从每天0点开始，每小时统计从0点至当前小时的交易额
         * CUMULATE(TABLE data, DESCRIPTOR(time_attr), step, size)
         * 测试跨天的case也是没问题的
         * 2020-04-15 08:05:10,4,C
         * 2020-04-15 08:30:03,3,p
         * 2020-04-15 10:00:00,2,w
         * 2020-04-16 02:00:03,3,p
         * 2020-04-16 03:01:00,2,o
         */
        sql = "select window_start, window_end, window_time, sum(price) as sum_price \n" +
                " from table(CUMULATE(table t_bid, descriptor(rt), interval '1' hours, interval '1' days)) \n" +
                " group by window_start, window_end, window_time";

        /**
         * 聚合之后的TopN
         * 每10min，交易总金额最大的两类商品的交易总金额、交易pv
         *  2020-04-15 08:05:10,4,C
         *  2020-04-15 08:07:03,3,p
         *  2020-04-15 08:09:03,3,p
         *  2020-04-15 08:09:33,6,a
         *  2020-04-15 10:00:00,2,w
         *  2020-04-15 12:00:03,3,p
         *
         * ==> 便于理解
         *  start, end,     item,       price,      pv
         *  0,      10,     c           4           1
         *  0,      10,     p           6           2
         *  0,      10,     a           6           1
         */
        sql =  "select\n" +
                " *\n" +
                "from (\n" +
                "     select\n" +
                "      *,\n" +
                "      row_number() over(partition by window_start, window_end order by price desc) as rn\n" +
                "     from (\n" +
                "        select\n" +
                "           window_start,\n" +
                "           window_end,\n" +
                "           item,\n" +
                "           sum(price) as price,\n" +
                "           sum(1) as pv\n" +
                "        from table(tumble(table t_bid, descriptor(rt), interval '10' minutes))\n" +
                "        group by window_start, window_end, item\n" +
                "     ) as t1\n" +
                ") as t2\n" +
                "where t2.rn <= 2";

        /**
         * 不聚合直接求TopN
         * 每10min，交易金额最大 的前两笔订单详情
         */
        sql = "select\n" +
                "  *\n" +
                "from (\n" +
                "     select\n" +
                "         --*,\n" + // 这里不能直接写*，因为rt字段和默认附加的window_time字段有冲突
                "         window_start, window_end, rt, item, price,\n" +
                "         row_number() over(partition by window_start, window_end order by price desc) as rn\n" +
                "     from table(tumble(table t_bid, descriptor(rt), interval '10' minutes))\n" +
                ") as t\n" +
                "where t.rn <= 2";
        tenv.executeSql(sql).print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid{
        private String bidtime;    // "2020-04-15 08:05:00.000"
        private double price;
        private String item;
    }
}
