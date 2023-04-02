package cn.doitedu.flink.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @author zengwang
 * @create 2023-03-22 23:33
 * @desc:
 */
public class _08_SinkOperator_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<Tuple4<Long, String, Long, Map<String, String>>> tuple4Stream =
                streamSource.map(value -> Tuple4.of(value.getGuid(), value.getEventId(), value.getTimestamp(), value.getEventInfo()))
                .returns(new TypeHint<Tuple4<Long, String, Long, Map<String, String>>>() {});

        // 默认4k的buffer，调用每个元素的toString()，map字段也会被toString
        // 对于嵌套格式的字段，还是把整行作为一个字段json存储，下游更好解析
        tuple4Stream.writeAsCsv("/Users/zeng.wang/Downloads/csv_sink", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }
}
