package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zengwang
 * @create 2023-03-09 12:39
 * @desc:
 */
public class _04_WordCount_LambdaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = streamEnv.readTextFile("flink_course/src/data/wc/input/wc.txt");

        // 先把句子变大写
        /*
         * 从map算子接受的MapFunction的接口实现来看，它是一个单个抽象方法的接口，
         * 所以这个接口的实现类的核心功能，就在它的方法上，就可以用lambda表达式来简洁实现
         *
         * streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return null;
            }
           })
         */

        // lambda表达式怎么写，看你实现接口方法，接收什么参数，返回什么结果
        // 然后就按lambda语法来表达, (参数1，参数2, ...) -> {函数体}
        // streamSource.map((value) -> {return value.toUpperCase();});

        // 一级简化: 由于上面的lambda表达式，参数列表只有一个，且函数体只有一行代码，就可以简化
        // streamSource.map(value -> value.toUpperCase());

        // 二级简化：由于上面的lambda表达式，函数体只有一行代码，且参数只使用了一次，则可以将方法调用，转成"方法引用"
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);

        // 然后切成单词 转成(单词, 1)进行打平
        /*upperCased.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });
        FlatMapFunction是单方法接口，使用lambda替换
        */

        // 因为out参数作为Collector，本身需要指明收集的数据类型，否则只能按Object处理了
        // flatMap算子得到的数据流 元素类型就是Collector<T>的泛型T
        // 注：和上面的泛型String不同，String是我们直接调方法后的返回类型，Flink自己就知道，这里Flink只知道返回了容器Collector<Object>

        /*SingleOutputStreamOperator<Object> wordAndOne = upperCased.flatMap((value, out) -> {
            String[] words = value.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        });

        */

        // 依然是一个单方法接口，使用lambda实现其方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperCased.flatMap(
            (String value, Collector<Tuple2<String, Integer>> out) -> {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            })
                //.returns(new TypeHint<Tuple2<String, Integer>>() {}) // 通过TypeHint 传达返回数据类型
                //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})) // 更通用的是传入TypeInformation，
                // 上面的TypeHint也是封装了TypeInformation
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // 利用工具Types的各种静态方法，来生成TypeInformation
                ;
        /**
         * new KeySelector<Tuple2<String, Integer>, String>() {
         *             @Override
         *             public String getKey(Tuple2<String, Integer> value) throws Exception {
         *                 return null;
         *             }
         *         }
         */
        // 按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(tp -> tp.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum("f1");
        sum.print();

        streamEnv.execute();
    }
}
