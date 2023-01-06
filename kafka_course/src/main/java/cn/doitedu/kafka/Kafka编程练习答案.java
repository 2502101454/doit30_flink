package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

/**
 * @author zengwang
 * @create 2022-12-04 20:11
 * @desc: 每五分钟统计到当时位置的 uv
 */
public class Kafka编程练习答案 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.load(Kafka编程练习答案.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "doit30-2");
        //props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "500");
        //props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("doit30-events"));

        //HashSet<Long> distinctUids = new HashSet<>();

        // 答案，但是这个方案是不能每次都准确切在5s的点上的，因为你这个是同步操作，应该使用多线程方案，一个线程拉数据，一个线程消费
        /*while (true) {
            Long timeStampInSeconds = System.currentTimeMillis() / 1000;
            if (timeStampInSeconds % 5 == 0) {
                System.out.println(String.format("current timestamp: %d, uv %d", timeStampInSeconds,
                        distinctUids.size()));
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                int partition = record.partition();
                long offset = record.offset();
                UserEvent event = JSON.parseObject(value, UserEvent.class);
                System.out.println("partition: " + partition + " offsets: " + offset + " value: " + event);
                distinctUids.add(event.getGuId());
            }
        }*/

        // 验证消费者的拉取逻辑：结合max.partition.fetch.bytes、fetch.max.bytes 参数来视情况，因此可以是一次拉取多个分区数据后返回，
        // 也可以是拉取一个分区数据后返回
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                int partition = record.partition();
                long offset = record.offset();
                UserEvent event = JSON.parseObject(value, UserEvent.class);
                System.out.println("partition: " + partition + " offsets: " + offset + " value: " + event);
            }

            System.out.println("一次拉取结束");
            Thread.sleep(10 * 1000);

        }
    }
}
