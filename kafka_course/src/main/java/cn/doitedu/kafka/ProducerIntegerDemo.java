package cn.doitedu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

/**
 * @author zengwang
 * @create 2022-11-20 20:53
 * @desc: kafka生产者api代码示例
 */
public class ProducerIntegerDemo {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        /*
        1.kafka中的message 是k-v结构的，(你发的的时候可以没有key)
        2.业务数据可以有各种类型的，但是在kafka内部是没有类型的，它底层就存数据的序列化字节。因此，你需要指定你的序列化方式
        */
        // 配置参数的第二种方式，这里会把上面的参数覆盖，所有不用注掉上面的代码
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //消息发送应答级别
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        // 构造一个生产者客户端
        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<Integer, Integer> message = new ProducerRecord<>("test_data_type", 10, 101);
        // 调用客户端去发送, 数据的发送动作在producer底层是异步的，调用.get()就是阻塞等待
        producer.send(message);
        Thread.sleep(100);
        producer.close();
    }
}
