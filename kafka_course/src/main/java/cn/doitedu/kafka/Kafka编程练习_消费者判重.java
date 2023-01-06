package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * @author zengwang
 * @create 2022-12-06 21:07
 * @desc: 需求，对于首次一个用户的行为，加flag字段 flag: 1。后续的行为flg: 0
 */
public class Kafka编程练习_消费者判重 {
    public static void main(String[] args) {
        // 启动消费者线程
        new Thread(new ConsumerRunnable_bloomFilter()).start();

    }
}

/**
 * 消费拉取数据的线程runnable
 */
class ConsumerRunnable_bloomFilter implements Runnable {
    BloomFilter<Long> bloomFilter;
    KafkaConsumer<String, String> consumer;
    public ConsumerRunnable_bloomFilter() {
        // 参数： 存啥类型的数据，预计多少条去重后的，容忍的假阳率(本来不存在，我判断存在)
        bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01);
        // kafka连接创建
        Properties props = new Properties();
        try {
            props.load(ConsumerRunnable.class.getClassLoader().getResourceAsStream("consumer.properties"));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "doit30-2");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        } catch (IOException e) {
            e.printStackTrace();
        }

        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList("doit30-events"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(80000));
            for (ConsumerRecord<String, String> record : records) {
                String eventJson = record.value();
                UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);
                //System.out.println();
                // 判断是否存在
                boolean mightContain = bloomFilter.mightContain(userEvent.getGuId());
                if (mightContain) {
                    userEvent.setFlag(0);
                } else {
                    userEvent.setFlag(1);
                    // 写入布隆
                    bloomFilter.put(userEvent.getGuId());
                }

                System.out.println(JSON.toJSON(userEvent));
            }
        }

    }
}
