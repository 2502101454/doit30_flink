package cn.doitedu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author zengwang
 * @create 2022-11-20 20:53
 * @desc: kafka生产者api代码示例
 */
public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        /*
        1.kafka中的message 是k-v结构的，(你发的的时候可以没有key)
        2.业务数据可以有各种类型的，但是在kafka内部是没有类型的，它底层就存数据的序列化字节。因此，你需要指定你的序列化方式
        */
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 配置参数的第二种方式，这里会把上面的参数覆盖，所有不用注掉上面的代码
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //消息发送应答级别
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        // 构造一个生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for(int i=0; i<500; i++) {
            // 将业务数据封装成客户端所能发送的格式，生产者代码中，可以根据数据进行判断，发送到不同的topic
            // 0: abc0
            // 1: abc1
            // 奇数发往abcx，偶数发往abcy
            ProducerRecord<String, String> message = null;
            if (i % 2 == 1) {
                message = new ProducerRecord<>("abcx",i + "", "abc" + i);
            } else {
                message = new ProducerRecord<>("abcy",i + "", "abc" + i);
            }
            // 调用客户端去发送, 数据的发送动作在producer底层是异步的，调用.get()就是阻塞等待
            producer.send(message);

            Thread.sleep(100);
        }

        // 生产者在发数据给topic之前，先是在自己本地缓存一下，强制把缓存发完
        // producer.flush();
        // 关闭客户端，会自动检查缓存
        producer.close();
    }
}
