package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.util.RandomPivotingStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.xml.stream.FactoryConfigurationError;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zengwang
 * @create 2023-03-15 12:56
 * @desc:
 */
public class _06_CustomSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventLog> dataStream = env.addSource(new MySourceFunction());
        dataStream.setParallelism(2);
        dataStream.map(JSON::toJSON).print();

        env.execute();
    }
}

// 自定义source 产生的数据是eventLog对象
class MySourceFunction implements SourceFunction<EventLog> {
    volatile boolean flag = true;
    // volatile 保证多线程修改同一个变量时候，线程能够及时获取到最新的值

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "itemShare", "itemClick", "itemCollect",
        "putBack", "wakeUp", "appClose"};
        HashMap<String, String> eventInfo = new HashMap<>();

        while (flag) {
            eventLog.setGuid(RandomUtils.nextLong(1, 1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(1024).toUpperCase());
            eventLog.setTimestamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);

            eventInfo.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfo);

            ctx.collect(eventLog);
            eventInfo.clear();

//            Thread.sleep(RandomUtils.nextInt(50, 150));
            Thread.sleep(1);
        }
    }

    // Job取消时，调用该方法
    @Override
    public void cancel() {
        flag = false;
    }
}



class MyRichSourceFunction extends RichSourceFunction<EventLog> {

    // source组件初始化时调用
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 调用父类的方法，获取上下文信息
        RuntimeContext context = getRuntimeContext();
        // 获取该subTask实例 所属的Task的Name
        String taskName = context.getTaskName();
        // 获取该subTask实例的 id
        int indexOfThisSubtask = context.getIndexOfThisSubtask();
    }

    // 组件关闭调用
    @Override
    public void close() throws Exception {
        super.close();

    }

    // source组件生成数据的过程，核心逻辑
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    // job取消时，调用
    @Override
    public void cancel() {

    }
}

