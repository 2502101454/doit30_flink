package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * @author zengwang
 * @create 2022-11-20 22:22
 * @desc:
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Kafka的消费者，默认是从所属组之前记录的偏移量开始消费，如果找不到先前记录的偏移量，则从如下参数配置的策略来确定消费的起始偏移量
        // offset重置策略: earliest、latest、none（没有重置策略，则找不到先前的偏移量时，就会报错）
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 设置消费者的组id
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-1");
        // 设置自动提交最新的消费位移
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //默认就是开启的
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"); // 自动提交的时间间隔，默认5000ms

        // 构造一个消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题(可以是多个)
        consumer.subscribe(Collections.singletonList("abcx"));

        // 总结：先手动，再继承先前组，最后走重置策略（api中: auto.offset.reset）
        // 显式指定消费的起始偏移量
        /*TopicPartition abcxP0 = new TopicPartition("abcx", 0);
        TopicPartition abcxP1 = new TopicPartition("abcx", 1);
        consumer.seek(abcxP0, 100);
        consumer.seek(abcxP1, 100);*/

        // 对数据进行业务逻辑处理
        // 循环往复拉取数据
        boolean condition = true;
        while (condition) {
            // 客户端拉取数据的时候，如果服务端没有数据响应，会保持连接等待服务端响应
            // poll中传入超时时长参数，是指等待的最大时长, ConsumerRecords<k, v>，一次拉回一批数据，但是条数不一定
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                // 用户的业务数据:
                String key = record.key();
                String value = record.value();

                // kafka内部的元数据:
                //本条数据所属的topic
                String topic = record.topic();
                //本条数据所属的partition
                int partition = record.partition();
                //本条数据的offset
                long offset = record.offset();
                //本条数据所在的分区leader的朝代纪年
                Optional<Integer> leaderEpoch = record.leaderEpoch();
                // 时间戳有两种类型：本条数据的创建时间(生产者)；本条数据的追加时间(broker写入log文件的时间)
                // 默认是createTime，获取追加时间则需要在创建topic的时候去改log.message.timestamp.type的参数
                TimestampType timestampType = record.timestampType();
                // 获取本条数据的对应类型的时间戳
                long timestamp = record.timestamp();

                //数据头:
                // 用户在生产者端写入数据时可以附加的header(用户自定义的)
                Headers headers = record.headers();

                System.out.println(String.format("数据key: %s, 数据value: %s, topic: %s, partition: %s, offset: %s," +
                        "leaderEpoch: %s, timestampType: %s, timeStamp: %s",
                        key, value, topic, partition, offset, leaderEpoch.get(), timestampType.name, timestamp));
            }
        }

        // 关闭客户端
        consumer.close();
    }
}
