package cn.doitedu.flink.task;

/**
 * @author zengwang
 * @create 2023-05-05 11:30
 * @desc:
 */
public class Task2 implements Runnable{
    @Override
    public void run() {
        // String data = receive();

        Mapper2 mapper2 = new Mapper2();
        // String res = mapper2.map(data);

        // channel.send(res)
        System.out.println("task2 被执行了");
    }
}
