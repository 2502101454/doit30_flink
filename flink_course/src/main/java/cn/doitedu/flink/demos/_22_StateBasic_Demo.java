package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2023-05-30 16:28
 * @desc:
 */
public class _22_StateBasic_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // a
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        /**
         * 需求：每来一条数据，输出字符串为: 此条数据 + 先前输出的结果
         *
         * 思路：自己维护一个状态(Raw State)，存上次输出的结果
         * 弊端：重启后状态丢失
         *  v1: 进行持久化，重启后读取持久化结果, 但是什么时候持久化？
         *      a.每来一条数据持久化一次？ 效率太低
         *      b.定期持久化，比较适合，但是存在丢失部分状态数据，举例: 内存中的acc都abcdef了，但上次持久化的结果还是abc，此时挂了
         *
         *  我们自己维护状态实现很麻烦，因此FLink设计了状态机制，帮我们托管程序的状态，解放程序员
         *  v2: 使用Flink State(托管状态)
         *      从Flink API中获取一个状态管理器，用这个状态管理器进行数据的增删改查操作
         */
        s1.map(new MapFunction<String, String>() {
            // 自己定义、管理的状态，持久化
            String acc = "";
            @Override
            public String map(String value) throws Exception {
                acc += value;
                return acc;
            }
        }).print();

        env.execute();
    }

}
