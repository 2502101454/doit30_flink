package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zengwang
 * @create 2023-08-01 20:01
 * @desc:
 */
public class Demo2_TableAPI {
    public static void main(String[] args) {

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                                                            .inStreamingMode()
                                                            .build();
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        Table table = tableEnv.from(TableDescriptor.forConnector("kafka") // 指定连接器
                .schema(Schema.newBuilder()                 // 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json") // 指定数据源的数据格式
                .option("topic", "doit30-3")  // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "hadoop102:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());


        // 利用table api进行查询计算
        table.groupBy($("gender"))
                .select($("gender"), $("age").avg())
                .execute()
                .print();

    }
}
