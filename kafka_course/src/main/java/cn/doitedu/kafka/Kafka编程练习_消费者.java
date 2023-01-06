package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zengwang
 * @create 2022-12-06 21:07
 * @desc:
 */
public class Kafka编程练习_消费者 {
    public static void main(String[] args) {
        // 想实现线程安全的HashSet，只能是用hashMap进行封装了
        ConcurrentHashMap<Long, String> guidMap = new ConcurrentHashMap<>();


        // 启动消费者线程
        new Thread(new ConsumerRunnable(guidMap)).start();

        // 再启动一个线程，每五秒读一次结果
        // 优雅一点来实现定时调度，可以用各种定时调度器(第三方的，也可以用jdk自带的: Timer)
        Timer timer = new Timer();
        // 多久后(0:立马)开始调用，并且之后每多长时间(10s)调用一次
        timer.scheduleAtFixedRate(new StatisticTask(guidMap), 0, 10000);
    }
}

/**
 * 消费拉取数据的线程runnable
 */
class ConsumerRunnable implements Runnable {
    ConcurrentHashMap<Long, String> guidMap;
    public ConsumerRunnable( ConcurrentHashMap<Long, String> guidMap) {
        this.guidMap = guidMap;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        try {
            props.load(ConsumerRunnable.class.getClassLoader().getResourceAsStream("consumer.properties"));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "doit30-2");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("doit30-events"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(80000));
            for (ConsumerRecord<String, String> record : records) {
                String eventJson = record.value();
                UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);
                //System.out.println();
                guidMap.put(userEvent.getGuId(), "");
            }
        }

    }
}

class StatisticTask extends TimerTask {
    ConcurrentHashMap<Long, String> guidMap;
    public StatisticTask( ConcurrentHashMap<Long, String> guidMap) {
        this.guidMap = guidMap;
    }

    @Override
    public void run() {
        System.out.println(String.format("%s 截至当前总用户数 %d", DateFormatUtils.format(new Date(),
                        "yyyy-MM-dd HH:mm:ss"), guidMap.size()));
    }
}