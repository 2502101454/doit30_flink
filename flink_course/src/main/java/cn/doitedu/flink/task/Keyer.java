package cn.doitedu.flink.task;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author zengwang
 * @create 2023-05-05 13:02
 * @desc:
 */
public class Keyer {
    private HashMap<String, List<Tuple2<String, Integer>>> map = new HashMap<>();

    /**
     * 这么理解KeyBy过于荒谬，忽略
     * {
     *     key: [(key, 1), (key, 1)...],
     *     ...
     * }
     * 实现逻辑：维护一个字典，对进来的数据进行分组，然后每次返回更新后的字典
     * @param tuple2
     * @return
     */
    public HashMap<String, List<Tuple2<String, Integer>>> keyBy(Tuple2<String, Integer> tuple2) {
        String key = tuple2.f0;
        if (map.containsKey(key)) {
            List<Tuple2<String, Integer>> values = map.get(key);
            values.add(tuple2);
        } else {
            List<Tuple2<String, Integer>> values = new ArrayList<>();
            values.add(tuple2);
            map.put(key, values);
        }

        return map;
    }
}
