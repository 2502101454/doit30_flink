package cn.doitedu.flink.task;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author zengwang
 * @create 2023-05-05 11:16
 * @desc:
 */
public class Mapper3 {
    public Tuple2<String, Integer> map(String s) {
        return Tuple2.of(s, 1);
    }
}
