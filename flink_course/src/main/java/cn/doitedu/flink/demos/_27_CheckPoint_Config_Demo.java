package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author zengwang
 * @create 2023-06-26 14:50
 * @desc:
 */
public class _27_CheckPoint_Config_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定ck的间隔时长(JM多久发一次barrier)；ck的算法模式，是否要对齐，这里是对齐的 即保证EOS
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointStorage("hdfs://hadoop102:8020/ckpt");
        // 设置job运行期间，允许最大的ck失败次数，超过则job挂掉
        config.setTolerableCheckpointFailureNumber(10);
        // 设置对齐模式时，快等慢的超时时长，如果超过此时长，慢的barrier还没到达，则认为这次ck失败
        config.setAlignedCheckpointTimeout(Duration.ofMillis(2000));
        // config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 同理设置ck模式
        // 设置算子在做ck时，两次ck的最小间隔时长，因为barrier一到达，也不一定要立马做ck，因为做ck的时候，算子会停止数据处理，
        // 如果barrier太密集了，导致ck太频繁，从而没工夫处理数据
        config.setMinPauseBetweenCheckpoints(2000);
        // 允许最大的未完成的(飞行中的)ck个数，举例：算子链路长，可能算子在处理barrier1对应的ck1，但是ck1并未被整体完成，
        // 前面的算子接着在处理barrier2对应的ck2，ck2也不算完成。如果超过上限了，则JM不会再触发新的ck了，直到这里完成飞行中的ck
        config.setMaxConcurrentCheckpoints(5);
        config.setCheckpointTimeout(3000); // 设置一个算子在一次ck执行过程中，总耗费时长的上限，如果超过，则本次ck认为失败
        // 取消job时，保留job的checkpoint数据，以便下次job重启可用于恢复
        config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        env.execute();
    }
}
