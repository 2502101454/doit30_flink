package cn.doitedu.flink.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zengwang
 * @create 2023-05-05 12:28
 * @desc:
 */
public class TaskRunner {
    public static void main(String[] args) {
        System.out.println(Long.MIN_VALUE);
        // TaskSlot 是一份资源(内存隔离)，同时只能提供给一个线程使用，因此槽共享的多个subTask虽然是多个线程()，但是真正执行起来，整个槽是串行的，
        // 只能切换执行subTask，因此TaskSlot`可以理解为`一个单线程的线程池

        // 一个Task实例是一个subtask，不同task的subTask共用一个slot线程，slot中，这些subTask切换执行

        // 简单类比，TaskSlot是一个线程池创建一个单线程的线程池，里面的任务被切换执行，submit后就会自己执行
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(new Task1());
        executorService.submit(new Task2());
        executorService.submit(new Task3());
        // executorService.awaitTermination();

//        // Task1 3个并行实例，每个并行实例叫做subTask
//        new Thread(new Task1()).start();
//        new Thread(new Task1()).start();
//        new Thread(new Task1()).start();
//        // Task2 3个并行实例
//        new Thread(new Task2()).start();
//        new Thread(new Task2()).start();
//        new Thread(new Task2()).start();
//        // Task3 3个并行实例
//        new Thread(new Task3()).start();
//        new Thread(new Task3()).start();
//        new Thread(new Task3()).start();
    }
}
