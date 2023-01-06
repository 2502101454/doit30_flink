package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zengwang
 * @create 2023-01-04 22:07
 * @desc: 利用mysql的事务机制来实现kafka consumer数据传输过程， 端到端的 exactly - once
 * 准备工作：
 *  1.创建topic
 *  kafka-topics.sh --create --topic user-info --partitions 3 \
 *  --replication-factor 2 --zookeeper hadoop103:2181
 *  2.创建mysql表：
 *  CREATE TABLE `stu_info` (
 *   `id` int(11) NOT NULL,
 *   `name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
 *   `age` int(11) DEFAULT NULL,
 *   `gender` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 *  3.创建记录消费位移的表
 *  CREATE TABLE `t_offsets` (
 *    `topic_partition` varchar(255) COLLATE utf8mb4_bin NOT NULL,
 *    `offset` bigint(255) DEFAULT NULL,
 *    PRIMARY KEY (`topic_partition`)
 *  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 */
public class Consumer实现ExactlyOnce手段1 {
    public static void main(String[] args) throws IOException, SQLException {
// 从配置文件中加载写的参数
        Properties props = new Properties();
        props.load(ConsumerDemo3.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-2");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // id,name,age,gender
        consumer.subscribe(Arrays.asList("user-info"));

        // 创建jdbc连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/abc",
                "root", "hadoop");
        // 业务数据sql
        PreparedStatement pstData = conn.prepareStatement("insert into stu_info values (?, ?, ?, ?)");
        // 利用mysql自身的幂等性更新偏移量 (设表的主键是 topic_partition，利用幂等性语法可减少开发量)
        // 不使用mysql幂等性来维护偏移量也可以，就是代码量大、效率比较低
        PreparedStatement pstOffset = conn.prepareStatement("INSERT INTO t_offsets VALUES(?, ?) ON DUPLICATE KEY UPDATE offset = ?");

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            // 遍历拉取到的一批数据
            for (ConsumerRecord<String, String> record: records) {
                String data = record.value();
                // 解析原始数据
                String[] fields = data.split(",");

                // 插入mysql
                pstData.setInt(1, Integer.parseInt(fields[0]));
                pstData.setString(2, fields[1]);
                pstData.setInt(3, Integer.parseInt(fields[2]));
                pstData.setString(4, fields[3]);

                pstData.execute();

                // kafka的偏移量要更新为下一次读取的那条记录，假设当前读的消息对应的偏移量是a,
                // 则要更新偏移量为a + 1

            }
        }

        pstData.close();
        conn.close();
        consumer.close();
    }
}
