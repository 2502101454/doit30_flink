package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * @author zengwang
 * @create 2022-11-27 16:09
 * @desc: 手动指定消费起始偏移量
 */
public class ConsumerDemo2 {
    public static void main(String[] args) throws IOException {
        // 从配置文件中加载写的参数
        Properties props = new Properties();
        props.load(ConsumerDemo2.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-2");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        // subscribe 订阅，会参与消费组的自动再均衡机制：为组内的消费者分配要消费的topic及其分区
        // consumer.subscribe(Collections.singletonList("test"));
        // 分区再均衡机制进行中...（后台异步操作）
        // 再均衡机制触发条件: 分组里面消费者数量变化了，或者订阅的主题数变化了，或者消费的分区数变化了，都会触发

        // 这里无意义的拉取一次数据，主要是为了确保，再均衡机制结束，分区分配已完成~
        // 该行语句拉到数据就返回
        // consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

        TopicPartition test0 = new TopicPartition("test", 0);
        // seek是指定要消费的分区的偏移量(但现在还没有说要把这个分区分配给你消费!)
        // consumer.seek(test0, 9);
        // 下面除了消耗我指定的分区，还消耗另外两个分区的数据，而且消费的起始偏移量也不是从9开始的(也不是严格的latest，也不是earliest)，就很迷。。
        // 老师教程中是自动创建的topic，是一个分区的
        // 违背设计初衷：如果你就要指定分区和位移，那么就不要参与消费组的自动再均衡机制，这个机制就是完全要由内部自动

        // 使用assign，不参与消费组的分区再均衡机制，手动指定topic 和分区
        consumer.assign(Collections.singletonList(test0));
        // seek() API是主要用于assign的分配方式
        // 手动指定partition 和offset 进行消费，结果是正确的，不会消费其他的分区
        consumer.seek(test0, 10);


        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record: records) {
                String key = record.key();
                String value = record.value();

                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                Optional<Integer> leaderEpoch = record.leaderEpoch();
                TimestampType timestampType = record.timestampType();
                long timestamp = record.timestamp();

                System.out.println(String.format("数据key: %s, 数据value: %s, topic: %s, partition: %s, offset: %s," +
                                "leaderEpoch: %s, timestampType: %s, timeStamp: %s",
                        key, value, topic, partition, offset, leaderEpoch.get(), timestampType.name, timestamp));
            }
        }
    }
}
