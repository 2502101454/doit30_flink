package cn.doitedu.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2023-06-04 17:29
 * @desc:
 */
public class _26_State_TTL_Demo { public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // HashMapStateBackend hashMapStateBackend = new HashMapStateBackend(); // 默认
    EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
    env.setStateBackend(embeddedRocksDBStateBackend);

    env.setParallelism(1);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

    // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
    env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);

    // 开启快照后，就需要指定快照数据的持久化存储位置
    env.getCheckpointConfig().setCheckpointStorage("file:///D:\\code_ship\\output\\ckp");

    // 开启  task级别故障自动 failover
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

    DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

    // keyBy + map达到一个效果：
    source.keyBy(s -> "0")
        .map(new RichMapFunction<String, String>() {
            // 疑问： 每个map算子维护一个ListState，内部自动区分key？还是每个key自身维护一个ListState对象？
            private ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.milliseconds(1000)) // 配置数据的存活时长1s
                        // 1.ttl时长
                        .setTtl(Time.milliseconds(4000)) // 配置数据的存活时长为4s，覆盖上面的配置

                        // 2.什么时候更新ttl
                        .updateTtlOnReadAndWrite() // 读、写都导致该条数据的ttl计时重置
                        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //.updateTtlOnCreateAndWrite() // 当插入和更新时，该条数据的ttl计时重置

                        // 3.时间语义设置(默认处理时间语义，暂未支持事件时间，新版本可能会有)
                        .useProcessingTime()
                        //.setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)

                        // 4.用户取状态数据时，对脏数据的处理
                        // 不允许返回已过期但尚未被清理的数据
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        // 允许返回已过期但尚未被清理的数据- 开发中一般都不用这个设置
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)

                        // 下面3种过期数据检查清除策略，不是覆盖关系，而是添加的关系
                        /* 增量清理 当状态数据被访问的时候，则对CopyOnWriteMap中的k进行遍历，清理对应的value中的过期的状态数据；
                            具体清理机制由如下两个参数指定:
                            cleanUpSize 一次遍历多少个k进行清理
                            runCleanupForEveryRecord 设为true，则cleanUpSize不起作用，即每次访问状态，都会把所有key都过一遍*/
                        .cleanupIncrementally(1000, false)
                        // 全量快照清理策略（在checkpoint时候，只有未过期的状态数据才会保存到快照文件中，它并不会清理算子本地的状态数据）
                        //.cleanupFullSnapshot()
                        // 在rocks DB作为状态后端时，rocksDB特有的compact机制中添加过期数据过滤器，以便在compact过程中清理过期的状态数据
                        //.cleanupInRocksdbCompactFilter(1000)
                        .build();

                ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("string", String.class);
                descriptor.enableTimeToLive(stateTtlConfig);
                //本状态管理器会执行ttl管理
                listState = runtimeContext.getListState(descriptor);

                // TODO.实验ttl机制，实验访问过期数据会重置其ttl吗？得搞定定时器间隔，掐点?
            }

            @Override
            public String map(String value) throws Exception {
                listState.add(value);
                Iterable<String> strings = listState.get();
                StringBuilder sb = new StringBuilder();
                for (String string : strings) {
                    sb.append(string);
                }
                return sb.toString();
            }
        })
        .print();

    env.execute();
}
}
