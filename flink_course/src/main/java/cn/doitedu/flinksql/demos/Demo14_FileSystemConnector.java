package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-31 11:22
 * @desc:
 */
public class Demo14_FileSystemConnector {
    public static void main(String[] args) throws Exception {
        /**
         * ·可读可写
         * ·作为 source 表时，支持持续监视读取目录下新文件，且每个新文件只会被读取一次
         * ·作为 sink 表时，支持 多种文件格式、分区、文件滚动、压缩设置等功能
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，让subtask为1 ，方便观察文件切分
        env.setParallelism(1);
        // 不开启ck，则都是xxx.in-progress.xxx文件，还是走先前streamAPI fileSink那一套
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 建表 fs_table 来映射 mysql中的flinktest.stu
        tenv.executeSql(
                "CREATE TABLE fs_table (\n" +
                        "  user_id STRING,\n" +
                        "  order_amount DOUBLE,\n" +
                        "  dt STRING,\n" +
                        "  `hour` STRING\n" +
                        ") PARTITIONED BY (dt, `hour`) WITH (\n" +
                        "  'connector'='filesystem',\n" +
                        "  'path'='file:///d:/filetable/',\n" +
                        "  'format'='json',\n" +
                        "  'sink.partition-commit.delay'='1 h',\n" +
                        "  'sink.partition-commit.policy.kind'='success-file',\n" +
                        "  'sink.rolling-policy.file-size' = '1M',\n" +
                        "  'sink.rolling-policy.rollover-interval'='10 second',\n" +
                        "  'sink.rolling-policy.check-interval'='10 second'\n" +
                        ")"
        );


        // u01,88.8,2022-06-13,14
        SingleOutputStreamOperator<Tuple4<String, Double, String, String>> stream = env
                .socketTextStream("hadoop102", 9999)
                .map(s -> {
                    String[] split = s.split(",");
                    return Tuple4.of(split[0], Double.parseDouble(split[1]), split[2], split[3]);
                }).returns(new TypeHint<Tuple4<String, Double, String, String>>() {
                });

        tenv.createTemporaryView("orders",stream);

        tenv.executeSql("insert into fs_table select * from orders");

    }
}
