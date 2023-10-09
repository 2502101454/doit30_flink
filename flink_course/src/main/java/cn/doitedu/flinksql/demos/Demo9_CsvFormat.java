package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zengwang
 * @create 2023-10-03 16:21
 * @desc:
 */
public class Demo9_CsvFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        /*
        1,|aaa|,|20|
        |2|,|bbb|,|22|
        3,|AA,10|
        4,NN,

        1.'csv.ignore-parse-errors' = 'true'
            解析出错 程序不退出，而是将对应字段置为null
        2.csv.disable-quote-character
            禁用引用字符拆包(丢弃)，默认是false，即开启引用字符丢弃功能
          csv.quote-character 指定啥是引用字符，默认双引号"
            引用字符是用户自定义的，如上引用字符'|'，包裹的值如|20|

          ！！注意，csv.quote-character 包裹的内容含csv 分隔符，也将整体作为一个字段的值
        3.csv.null-litera : NN
        csv文件中，内容不方便表示null值，因此人们常规定某个值代表null，比如NN ，代表NULL
         */
        tenv.executeSql("create table t_csv ("
        + " id int,                             "
        + " name string,                        "
        + " age int                             "
        + " ) with (                            "
        + " 'connector' = 'filesystem',         "
        + " 'path' = 'data/csv',                "
        + " 'format' = 'csv',                    "
        + " 'csv.ignore-parse-errors' = 'true',  "
        + " 'csv.quote-character' = '|',         "
        + " 'csv.null-literal' = 'NN'         "
        + ")                                    "
        );
        tenv.executeSql("desc t_csv").print();
        tenv.executeSql("select * from t_csv").print();
    }
}
