package cn.doitedu.flink.task;

/**
 * @author zengwang
 * @create 2023-05-05 11:26
 * @desc:
 */
public class Task1 implements Runnable{
    @Override
    public void run() {
        // 从上游接收数据
        // String data = receive();

        // 算子逻辑
        Mapper1 mapper1 = new Mapper1();
        //String res = mapper1.map(data);

        // 把结果发给下游
        // channel.send(res);
        System.out.println("task1 休眠");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
