package cn.doitedu.flink.task;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.List;

/**
 * @author zengwang
 * @create 2023-05-05 11:32
 * @desc:
 */
public class Task4 implements Runnable{
    @Override
    public void run() {
        // String data = receive();
        String data = "hello";
        // 算子链
        Mapper1 mapper1 = new Mapper1();
        Mapper3 mapper3 = new Mapper3();
        Keyer keyer = new Keyer();

        String res1 = mapper1.map(data);
        Tuple2<String, Integer> tuple2 = mapper3.map(res1);
        HashMap<String, List<Tuple2<String, Integer>>> res = keyer.keyBy(tuple2);
        System.out.println(res);
    }

    public static void main(String[] args) {
        Task4 task4 = new Task4();
        task4.run();
    }
}
