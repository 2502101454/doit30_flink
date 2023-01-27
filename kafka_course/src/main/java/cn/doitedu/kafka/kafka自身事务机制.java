package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * @author zengwang
 * @create 2023-01-27 15:28
 * @desc: 从kafka的topic-a中读取数据，处理（把读到的数据转大写），处理结果写入kafka的topic-b
 * 利用kafka自身的事务机制，来实现 端到端的eos语义
 * 核心点：让消费的偏移量记录更新 和 生产端的数据落地，绑定再一个事务中
 */
public class kafka自身事务机制 {
    public static void main(String[] args) {
        Properties props = new Properties();
        /**
         * 消费者参数
         */
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop103:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "trans-001");
        // 关闭消费者的消费位移自动提交机制
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        /**
         * 生产者
         */
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop104:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "x001");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "4");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        consumer.subscribe(Arrays.asList("topic-a"));
        // 初始化事务
        producer.initTransactions();

        // 开始消费数据，做业务处理
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            // 另一种提交位移的方法： 记录分区消费对应的位移
            HashMap<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

            // 如果要对本次拉取的所有数据处理绑定在一个事务中，则在此处开启事务
            producer.beginTransaction();
            // 也可以针对一个分区提交事务，或者一条数据提交事务，回滚时，重复读取的数据会少些，但是这样你的吞吐量会变小
            try {
                // 换个遍历方法
                // 从拉取到的数据中，获取都由哪些TopicPartition
                Set<TopicPartition> topicPartitionSet = records.partitions();
                // 遍历每个分区
                for (TopicPartition topicPartition : topicPartitionSet) {
                    // 从拉取的数据中，单独取本分区的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // 业务处理逻辑
                        String result = record.value().toUpperCase();
                        // 把处理结果写出去
                        ProducerRecord<String, String> resultRecord = new ProducerRecord<>("topic-b", result);
                        producer.send(resultRecord);

                        // 记录本分去本条消息的offset 到 offsetMap中
                        offsetMap.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
                    }


                }
                // 手动提交偏移量：本次拉取的数据的最后的offset + 1
                consumer.commitSync(); // 会自动计算本批拉取数据中的每个分区的最大消费offset，来得到每个分区要提交的消费位移
                // consumer.commitSync(offsetMap); // 或者按照自己想要的各个分区消费位移来提交

                // 提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                // 如果上面的一批数据处理过程中任意时刻发生异常，则放弃本次事务
                // 下游可以通过设置isolation_level = read_committed来避开本次产生的"脏"数据
                producer.abortTransaction();
            }
        }





    }
}
