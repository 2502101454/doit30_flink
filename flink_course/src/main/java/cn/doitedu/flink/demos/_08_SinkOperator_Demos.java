package cn.doitedu.flink.demos;

import cn.doitedu.flink.avro.schema.AvroEventLog;
import com.alibaba.fastjson.JSON;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zengwang
 * @create 2023-03-22 23:33
 * @desc:
 */
public class _08_SinkOperator_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000 * 10, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zeng.wang/Downloads/ckpt");
        env.setParallelism(2);

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<Tuple4<Long, String, Long, Map<String, String>>> tuple4Stream =
                streamSource.map(value -> Tuple4.of(value.getGuid(), value.getEventId(), value.getTimestamp(), value.getEventInfo()))
                .returns(new TypeHint<Tuple4<Long, String, Long, Map<String, String>>>() {});

        // 默认4k的buffer，调用每个元素的toString()，map字段也会被toString
        // 对于嵌套格式的字段，还是把整行作为一个字段json存储，下游更好解析
        // tuple4Stream.writeAsCsv("/Users/zeng.wang/Downloads/csv_sink", FileSystem.WriteMode.OVERWRITE);

        // 应用 StreamFileSink算子，来将数据输出到文件系统
        /*
         * 1.输出 行格式
         */
        // 构建一个FileSink对象
        FileSink<String> rowSink = FileSink
                .forRowFormat(new Path("/Users/zeng.wang/Downloads/file_sink"), new SimpleStringEncoder<String>("utf-8"))
                // 文件的滚动策略(间隔时长10s，或者文件大小达到1M，进行文件切分)
                .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(1000 * 10).withMaxPartSize(5 * 1024 * 1024).build())
                // 分桶策略(划分子文件夹)， 默认按小时分桶
                .withBucketAssigner(new DateTimeBucketAssigner<String>())
                .withBucketCheckInterval(5) // 5 ms 检查一次当前是否可以分桶
                // 输出的文件名配置
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".txt").build())
                .build();

        //streamSource.map(JSON::toJSONString)
                        // .addSink() SinkFunction 实现类用addSink()来添加
                                //.sinkTo(rowSink); // Sink实现类 用sinkTo来添加
        /*
         * 2.输出 列格式
         * 要生成parquet，parquet自带schema，因此需要先定义Schema，而定义Schema有多种方式:
         *
                2.1 使用avro的Schema API创建，此方式麻烦
                Schema schema = Schema.createRecord("id", "用户id", "cn.doitedu.User", true);
                ParquetAvroWriters.forGenericRecord(schema)

                2.2 编写Schema的配置文件(avsc), 生成java Bean代表Schema(必须要继承SpecificRecordBase)
                ParquetAvroWriters.forSpecificRecord()

                2.3 使用用户自己Bean 反射创建Schema
        */

        /* 方式2.2
        // 理解: 这就是一个parquet的Schema
        ParquetWriterFactory<AvroEventLog> writerFactory1 = ParquetAvroWriters.forSpecificRecord(AvroEventLog.class);


        FileSink<AvroEventLog> parquetSink1 = FileSink.forBulkFormat(new Path("/Users/zeng.wang/Downloads/file_sink1"), writerFactory1)
                // 列式存储，结构复杂，故无法根据文件大小、时间间隔随意截断做文件切分;
                // 只有根据Checkpoint来做一个重操作，进行文件归档切分
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<AvroEventLog>())
                .withBucketCheckInterval(5)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".parquet").build())
                .build();

        streamSource.map(bean -> {
            // java的泛型类型 是 "不变" 场景，因此只能单拉出来构造CharSequence的 Map
            HashMap<CharSequence, CharSequence> map = new HashMap<>();
            for (Map.Entry<String, String> entry : bean.getEventInfo().entrySet()) {
                map.put(entry.getKey(), entry.getValue());
            }
            return new AvroEventLog(bean.getGuid(), bean.getSessionId(), bean.getEventId(), bean.getTimestamp(), map);
        })//.returns(AvroEventLog.class)
                .sinkTo(parquetSink1);

        */

        // 方式2.3 最简单常用
        ParquetWriterFactory<EventLog> writerFactory2 = ParquetAvroWriters.forReflectRecord(EventLog.class);
        FileSink<EventLog> parquetSink2 = FileSink.forBulkFormat(new Path("/Users/zeng.wang/Downloads/file_sink2"), writerFactory2)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>())
                .withBucketCheckInterval(5)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".parquet").build())
                .build();
        streamSource.sinkTo(parquetSink2);

        env.execute();
    }
}
