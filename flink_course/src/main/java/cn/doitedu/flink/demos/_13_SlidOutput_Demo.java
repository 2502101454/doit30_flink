package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zengwang
 * @create 2023-04-25 23:22
 * @desc:
 * 侧流输出
 *
 */
public class _13_SlidOutput_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);
        // 开启Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage("file://D:\\code_ship\\output");

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             *
             * @param value EventLog是输入数据
             * @param ctx 上下文，提供`侧输出`功能
             * @param out 主流输出的收集器
             * @throws Exception
             */
            @Override
            public void processElement(EventLog value, ProcessFunction<EventLog, EventLog>.Context ctx,
                                       Collector<EventLog> out) throws Exception {
                // 需求，把appLaunch、putBack 单独输出到侧流，其余数据输出到主流;
                String eventId = value.getEventId();
                if ("appLaunch".equals(eventId)) {
                    ctx.output(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)), value);
                }
                // putBack侧流转换类型为String
                if ("putBack".equals(eventId)) {
                    ctx.output(new OutputTag<String>("back", TypeInformation.of(String.class)), JSON.toJSONString(value));
                }
                out.collect(value);
            }
        });

        DataStream<String> back = processed.getSideOutput(new OutputTag<String>("back", TypeInformation.of(String.class)));
        back.print("back");
        DataStream<EventLog> launch = processed.getSideOutput(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)));
        launch.print("launch");

        processed.print("main stream");
        env.execute();
    }
}
