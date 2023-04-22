package cn.doitedu.flink.demos;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * @author zengwang
 * @create 2023-04-22 20:24
 * @desc:
 */
public class _09_StreamFileSinkOperator_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 开启Checkpoint
        env.enableCheckpointing(1000 * 10, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zeng.wang/Downloads/ckpt");

        // 演示方式1，其他方式参考_08中的案例
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 定义GenericRecord用的Schema
        Schema schema = SchemaBuilder.builder()
                // namespace 是包名， record是生成的javaBean的类名
                .record("DataRecord")
                .namespace("cn.doitedu.flink.avro.schema")
                .doc("用户行为事件的schema")
                .fields()
                    .requiredInt("gid")
                    .requiredLong("ts")
                    .requiredString("eventId")
                    .requiredString("sessionId")
                    .name("eventInfo")
                        .type()
                        .map()
                        .values()
                        .type("string")
                        .noDefault()
                .endRecord();


        // 定义parquet writer以及 Flink算子--套子==》parquetSink
        ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);
        FileSink<GenericRecord> parquetSink = FileSink.forBulkFormat(new Path("/Users/zeng.wang/Downloads/file_sink3"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd--HH"))
                .withBucketCheckInterval(5)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").build())
                .build();

        // 自定义Schema可以精细化控制, 你的javaBean和要写入到Parquet中的schema 是两回事，中间的映射由你设置
        SingleOutputStreamOperator<GenericRecord> recordStream = streamSource.map(
                (MapFunction<EventLog, GenericRecord>) value -> {
                    GenericData.Record record = new GenericData.Record(schema);
                    record.put("gid", (int) value.getGuid());
                    record.put("ts", value.getTimestamp());
                    record.put("eventId", value.getEventId());
                    record.put("sessionId", value.getSessionId());
                    record.put("eventInfo", value.getEventInfo());
                    return record;
                }).returns(new GenericRecordAvroTypeInfo(schema));
        // 由于avro自身就是搞序列化的，因此它的相关类、对象需要使用avro的序列化器，不支持JDK默认的序列化(没继承Serializable)
        // 而Flink中默认使用JDK的序列化器，所以这里得显式指定AvroTypeInfo 来提供Avro的序列化器

        recordStream.sinkTo(parquetSink);

        env.execute();


    }
}
