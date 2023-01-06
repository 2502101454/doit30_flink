package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * @author zengwang
 * @create 2022-11-27 16:09
 * @desc: 消费组分区再均衡机制观察，配合server命令行消费者一起
 */
public class ConsumerDemo3 {
    public static void main(String[] args) throws IOException {
        // 从配置文件中加载写的参数
        Properties props = new Properties();
        props.load(ConsumerDemo3.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-2");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        // reb-1 主题：3个分区
        // reb-2 主题：2个分区
        consumer.subscribe(Arrays.asList("reb-1", "reb-2"), new ConsumerRebalanceListener() {
            // 再均衡过程中，消费者会被取消先前所分配的主题、分区
            // 取消之后，consumer底层会调用如下方法
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("我被取消了如下主题、分区: " + partitions);
            }

            // 再均衡过程中，消费者会被重新分配到新的主题、分区
            // 分配好了新的主题、分区后，consumer底层会调用如下方法
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("我又被分配了如下主题、分区: " + partitions);
            }
        });

        // 再均衡机制是异步发生的，因此为了保证主进程不退出，这里死循环。再均衡机制执行的过程中，会来通知主进程
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        }
    }
}
