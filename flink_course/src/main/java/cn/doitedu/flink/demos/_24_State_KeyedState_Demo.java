package cn.doitedu.flink.demos;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zengwang
 * @create 2023-05-30 16:28
 * @desc:
 */
public class _24_State_KeyedState_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        /**
         * 需求：每来一条数据，按照key分组进行输出：此条数据 + 先前输出的结果
         * 思路：使用Flink KeyedState ，为每一个key维护一个state，记录先前该key的输出结果
         *
         * 三步走:
         *   A.设置状态的Checkpoint 和 task 重启策略
         *   B.创建状态
         *   C.使用状态
         */

        // A开启Flink State Checkpoint机制(快照的周期，快照的模式--默认是EOC的)
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 一般指定到HDFS
        env.getCheckpointConfig().setCheckpointStorage("file:///D:\\code_ship\\output\\ckp");

        // A开启 task级别的failover
        // 默认是不会自动failover，即RestartStrategies.noRestart()，task出故障则整个job失败
        // 设置重启策略：固定重启上限 和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // a
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        s1.keyBy(string -> string)
                /*.map(new RichMapFunction<String, String>() {
                    private ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        RuntimeContext runtimeContext = getRuntimeContext();

                        // 通过context获取keyedState
                        listState = runtimeContext.getListState(new ListStateDescriptor<String>("string", String.class));
                        // TODO: 使用不同的结构的state
                        //runtimeContext.getState()
                        //runtimeContext.getMapState()
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
                })*/
                .map(new WZMapFunction())
                .print();

        env.execute();
    }
}

/**
 * 要使用operatorState，需要用户自己的function类实现CheckpointedFunction，
 * 在initializedState中，去拿operator state存储器
 */
class WZMapFunction implements MapFunction<String, String>, CheckpointedFunction {
    // C
    ListState<String> listState;

    @Override
    public String map(String value) throws Exception {
        // 状态还是在内存中存储，但是出异常了，就没法做Checkpoint了，因此还是上次Checkpoint，重启后回丢数据；
        // 就算颠倒如下两个语句的顺序，也没啥意义，x毕竟来过了；
        listState.add(value);
        if (value.equals("x") && RandomUtils.nextInt(1, 15) % 4 == 0) {
            throw new Exception("哈哈哈哈, 出错了");
        }

        // 拼接历史以来的字符串
        Iterable<String> strings = listState.get();
        StringBuilder sb = new StringBuilder();
        for (String string : strings) {
            sb.append(string);
        }
        return sb.toString();
    }

    /**
     * 系统对Flink State做快照(持久化)前，会调用此方法，用户利用此方法可对状态数据做操作
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("checkpoint 触发了，Checkpoint Id" + context.getCheckpointId());
    }

    /**
     * 算子任务在启动时，调用此方法，进行状态数据的初始化
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 从Context拿到拿到状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        // 定义状态存储结构的描述器
        ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("strings", String.class);
        // B.根据描述器获取状态管理器
        listState = operatorStateStore.getListState(stateDescriptor);
        /**
         * getListState： task挂掉后重启时，会自动加载最近一次的快照数据
         * (底层快照是按照jobId分目录存储的，新提交的job，jobId就变了，因此重启Job就无法加载先前的State快照了)
         */

    }
}
