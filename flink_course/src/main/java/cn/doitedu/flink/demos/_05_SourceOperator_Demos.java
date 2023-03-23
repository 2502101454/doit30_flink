package cn.doitedu.flink.demos;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author zengwang
 * @create 2023-03-15 09:52
 * @desc:
 */
public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        /*
         * 从集合得到数据流，这里不做演示
         */

        /*
         * 从文件得到数据流
         */

        //
        DataStreamSource<String> fileSource = env.readTextFile("flink_course/src/data/wc/input/wc.txt", "utf-8");
        fileSource.map(String::toUpperCase);//.print();

        // FileProcessingMode.PROCESS_ONCE: 对文件只读一次，只计算一次程序就退出
        // FileProcessingMode.PROCESS_CONTINUOUSLY： 周期性监视文件，一旦文件内容变化发生变化，就会对整个内容再读取计算一遍
        DataStreamSource<String> filSource2 = env.readFile(new TextInputFormat(null),
                "flink_course/src/data/wc/input/wc.txt",
                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        filSource2.map(String::toUpperCase).print();


        // 触发执行，运行所有有sink算子的算子链
        env.execute();
    }
}
