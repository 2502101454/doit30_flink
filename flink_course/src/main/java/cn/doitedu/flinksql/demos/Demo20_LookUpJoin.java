package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
public class Demo20_LookUpJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 建表映射mysql中的flinktest.stu2
        tenv.executeSql(
                "create table customers(\n" +
                        "   id  int primary key,\n" +
                        "   name string,\n" +
                        "   gender string\n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://hadoop102:3306/flinktest',\n" +
                        "  'table-name' = 'stu2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'hadoop' \n" +
                        ")"
        );

        // userId,item,price
        SingleOutputStreamOperator<OrderInfo> orders = env.socketTextStream("hadoop102", 9999).map(s -> {
            String[] arr = s.split(",");
            return new OrderInfo(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]));
        });

        // 创建主表（需要声明处理时间属性字段）
        tenv.createTemporaryView("orders", orders, Schema.newBuilder()
                        .column("userId", DataTypes.INT())
                        .column("item", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .columnByExpression("pt", "proctime()")
                .build());

        tenv.executeSql("-- enrich each order with customer information\n" +
                "SELECT o.*, c.*\n" +
                "FROM orders AS o\n" +
                "  JOIN customers FOR SYSTEM_TIME AS OF o.pt AS c\n" + // 这句语法写死的，记住就是代表 从表
                "    ON o.userId = c.id").print();

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderInfo {
        public int userId;
        public String item;
        public double price;
    }

}
