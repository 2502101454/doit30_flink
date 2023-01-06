package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @author zengwang
 * @create 2022-12-04 16:47
 * @desc:
 *
 * 需求：
 *   写一个生产者，不断去生产 “用户行为事件”数据，写入kafka
 *   {"guid": 1, "eventId": "pageView", "timestamp": 1670143843}
 *   {"guid": 2, "eventId": "addCart", "timestamp": 1670143843}
 *   {"guid": 3, "eventId": "appLaunch", "timestamp": 1670143843}
 *   ...
 *
 *   写一个消费者，不断消费上述数据，每五分钟统计到当时位置的 uv
 */
public class Kafka编程练习 {
    public static void main(String[] args) throws IOException, InterruptedException {
        MyDataGen gen = new MyDataGen();
        gen.genData();
    }
}

class MyDataGen {

    KafkaProducer<String, String> producer;

    public MyDataGen() throws IOException {
        Properties properties = new Properties();
        properties.load(MyDataGen.class.getClassLoader().getResourceAsStream("producer.properties"));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //消息发送应答级别

        producer = new KafkaProducer(properties);
    }

    public void genData() throws InterruptedException {
        UserEvent userEvent = new UserEvent();
        while (true) {
            // 造一条假数据
            userEvent.setGuId(RandomUtils.nextInt(1, 1000));
            userEvent.setEventId(RandomStringUtils.randomAlphabetic(5, 8));
            userEvent.setTimestamp(System.currentTimeMillis());

            // 转json
            String jsonString = JSON.toJSONString(userEvent);
            // 将业务数据封装成producerRecord对象
            ProducerRecord<String, String> record = new ProducerRecord<>("doit30-events", jsonString);

            // 用producer写入kafka
            producer.send(record);

            Thread.sleep(RandomUtils.nextInt(200, 1500));
        }
    }
}

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
class UserEvent {
    private long guId;
    private String eventId;
    private long timestamp;
    private Integer flag;
}