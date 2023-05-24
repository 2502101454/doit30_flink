package cn.doitedu.flink.task;

/**
 * @author zengwang
 * @create 2023-05-05 11:32
 * @desc:
 */
public class Task3 implements Runnable{
    @Override
    public void run() {
        // String data = receive();
        String data = "hello";

        // 算子链
        Mapper1 mapper1 = new Mapper1();
        Mapper2 mapper2 = new Mapper2();

        String res1 = mapper1.map(data);
        String res2 = mapper2.map(res1);
        System.out.println(res2);
//        channel.send(res2);
        System.out.println("task3 被执行了");
    }

    public static void main(String[] args) {
        Task3 task3 = new Task3();
        task3.run();
    }
}
