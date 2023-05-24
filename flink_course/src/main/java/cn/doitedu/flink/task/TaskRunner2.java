package cn.doitedu.flink.task;

/**
 * @author zengwang
 * @create 2023-05-05 12:28
 * @desc:
 */
public class TaskRunner2 {
    public static void main(String[] args) {
        // Task1 6个并行实例，每个并行实例叫做subTask
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
    }
}
