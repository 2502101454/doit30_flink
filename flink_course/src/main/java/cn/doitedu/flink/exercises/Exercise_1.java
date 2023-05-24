package cn.doitedu.flink.exercises;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/**
 * @author zengwang
 * @create 2023-05-03 21:12
 * @desc:
 */

public class Exercise_1 {
    public static void main(String[] args) throws Exception {
        /**
        * 流1 ：
        * “id,eventId,cnt”
        * 1,event01,3
        * 1,event02,2
        * 2,event02,4
        * 流2 ：
        * “id,gender,city”
        * 1, male, shanghai
        * 2, female, beijing
        *
        *
        * 需求：
        * 1 , 将流1的数据展开
        * 比如，一条数据： 1,event01,3
        * 需要展开成3条:
        * 1,event01,随机数1
        * 1,event01,随机数2
        * 1,event01,随机数3
        *
        * 2 , 流1的数据，还需要关联上 流2 的数据  （性别，城市）
        * 并且把关联失败的流1的数据，写入一个侧流；否则输出到主流
        * 3 , 对主流数据按性别分组， 取 最大随机数所在的那一条数据 作为结果输出
        * 4 , 把主流处理结果，写入  mysql， 并实现幂等更新
        * 5 , 把侧流处理结果，写入 文件系统，并写成 parquet格式
        * */

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 开启Checkpoint
        env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zeng.wang/Downloads/ckpt");

        env.setParallelism(2);
        // 流1 准备
        // id, event, cnt
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        SingleOutputStreamOperator<EventCount> eventCountStream = stream1.map(new MapFunction<String, EventCount>() {
            @Override
            public EventCount map(String value) throws Exception {
                String[] split = value.split(",");
                return new EventCount(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
            }
        });

        // 流2 准备
        // id, gender, city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<UserInfo> userInfoStream = stream2.map(value -> {
            String[] split = value.split(",");
            return new UserInfo(Integer.parseInt(split[0]), split[1], split[2]);
        }).returns(TypeInformation.of(UserInfo.class));


        /**
         * 需求1，根据eventCount中的count次数，展开event
         */
        SingleOutputStreamOperator<EventCount> expandEventStream = eventCountStream.flatMap(new FlatMapFunction<EventCount, EventCount>() {
            @Override
            public void flatMap(EventCount value, Collector<EventCount> out) throws Exception {
                int count = value.getCnt();
                for (int i = 0; i < count; i++) {
                    out.collect(new EventCount(value.getId(), value.getEventId(), RandomUtils.nextInt(10, 100)));
                }
            }
        });

        // 状态描述
        MapStateDescriptor<Integer, UserInfo> stateDesc = new MapStateDescriptor<>("stateDesc",
                TypeInformation.of(Integer.class), TypeInformation.of(UserInfo.class));

        BroadcastStream<UserInfo> broadcastStream = userInfoStream.broadcast(stateDesc);
        BroadcastConnectedStream<EventCount, UserInfo> connect = expandEventStream.connect(broadcastStream);

        /**
         * 需求2，展开后的事件流关联维度流，将关联失败的数据写入侧流，关联成功的写入主流
         */
        OutputTag<EventUserInfo> joinFailure = new OutputTag<>("joinFailure", TypeInformation.of(EventUserInfo.class));
        SingleOutputStreamOperator<EventUserInfo> eventUserStream = connect.process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {
            @Override
            public void processElement(EventCount value, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.ReadOnlyContext ctx,
                                       Collector<EventUserInfo> out) throws Exception {
                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDesc);
                UserInfo userInfo = broadcastState.get(value.getId());
                if (userInfo == null) {
                    // 关联失败，写入侧流
                    ctx.output(joinFailure, new EventUserInfo(value.getId(), value.getEventId(), value.getCnt(), null, null));
                } else {
                    // 关联成功写入主流
                    out.collect(new EventUserInfo(value.getId(), value.getEventId(), value.getCnt(), userInfo.getGender(), userInfo.getCity()));
                }
            }

            @Override
            public void processBroadcastElement(UserInfo value, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.Context ctx,
                                                Collector<EventUserInfo> out) throws Exception {
                BroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDesc);
                broadcastState.put(value.getId(), value);
            }
        });

        eventUserStream.print();
        DataStream<EventUserInfo> joinFailureStream = eventUserStream.getSideOutput(joinFailure);
        // joinFailureStream.print("Join failed:");

        /**
         * 需求3. 对join上的数据按照性别分组，取随机数最大的那一条数据，进行输出
         */
        SingleOutputStreamOperator<EventUserInfo> maxCnt = eventUserStream.keyBy(obj -> obj.getGender()).maxBy("cnt");

        /**
         * 需求4. 主流输入mysql: id(int), event_id, cnt(int), gender, city;  实现幂等更新
         */
        SinkFunction<EventUserInfo> jdbcSink = JdbcSink.sink(
                "insert into user_event_info values(?, ?, ?, ?, ?) on duplicate key update event_id=?, cnt=?, gender=?, city=?",
                new JdbcStatementBuilder<EventUserInfo>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventUserInfo eventUserInfo) throws SQLException {
                        stmt.setInt(1, eventUserInfo.getId());
                        stmt.setString(2, eventUserInfo.getEventId());
                        stmt.setInt(3, eventUserInfo.getCnt());
                        stmt.setString(4, eventUserInfo.getGender());
                        stmt.setString(5, eventUserInfo.getCity());

                        stmt.setString(6, eventUserInfo.getEventId());
                        stmt.setInt(7, eventUserInfo.getCnt());
                        stmt.setString(8, eventUserInfo.getGender());
                        stmt.setString(9, eventUserInfo.getCity());

                    }
                },
                // withBatchSize(10): 攒10条才开始写一次DB
                JdbcExecutionOptions.builder().withBatchSize(1).withMaxRetries(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/wz_test")
                        .withUsername("root")
                        .withPassword("12345678")
                        .build()
        );
        maxCnt.print("max:");
        maxCnt.addSink(jdbcSink);

        /**
         * 需求5，侧流数据写入文件系统 Parquet
         *
         * 试验时，forReflectRecord(EventUserInfo.class); 生成的Schema，默认是不允许有null值，还是使用精细化控制的好
         */

        // 定义GenericRecord用的Schema
        Schema schema = SchemaBuilder.builder()
                // namespace 是包名， record是生成的javaBean的类名
                .record("EventUserInfoRecord")
                .namespace("cn.doitedu.flink.avro.schema")
                .doc("用户行为事件的schema")
                .fields()
                .requiredInt("id")
                .requiredString("event_id")
                .requiredInt("cnt")
                .nullableString("gender", "null")
                .nullableString("city", "null")
                .endRecord();

        ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);
        FileSink<GenericRecord> parquetSink = FileSink.forBulkFormat(new Path("/Users/zeng.wang/Downloads/flink_exercise_1"), writerFactory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>())
                .withBucketCheckInterval(5)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".parquet").build())
                .build();

        SingleOutputStreamOperator<GenericRecord> genericStream = joinFailureStream.map(bean -> {
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("id", bean.getId());
            genericRecord.put("event_id", bean.getEventId());
            genericRecord.put("cnt", bean.getCnt());
            genericRecord.put("gender", bean.getGender());
            genericRecord.put("city", bean.getCity());
            return genericRecord;
        }).returns(new GenericRecordAvroTypeInfo(schema));

        genericStream.sinkTo(parquetSink);
        env.execute();
    }
}
