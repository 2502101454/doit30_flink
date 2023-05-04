package cn.doitedu.flink.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author zengwang
 * @create 2023-04-28 17:20
 * @desc:
 */
public class _14_StreamConnect_Union_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(2);

        // 输入数字字符串
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        // 输入字母字符串
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);
        
        /**
         * 流的 connect
         */
        ConnectedStreams<String, String> connectedStreams = stream1.connect(stream2);
        // CoMapFunction<左流类型, 右流类型, 算子输出的流类型>
        SingleOutputStreamOperator<String> map = connectedStreams.map(new CoMapFunction<String, String, String>() {
            // 共同的状态数据
            String prefix = "doitedu_";

            /**
             * 对左流进行处理
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map1(String value) throws Exception {
                // 转数字 * 10，再返回字符串
                return prefix + (Integer.parseInt(value) * 10);
            }


            /**
             * 对右流进行处理
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map2(String value) throws Exception {
                // 返回字符串的大写
                return prefix + value.toUpperCase();
            }
        });

//        map.print();

        /**
         * 流的 union
         * 参与union的流，必须数据类型一致
         */
        // stream1.map(Integer::parseInt).union(stream2); // union左右两边流类型不一致，不通过
        DataStream<String> unioned = stream1.union(stream2);
        unioned.map(s -> "doitedu_" + s).print();

        env.execute();

    }
}
