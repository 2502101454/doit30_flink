package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.rmi.server.ExportException;
import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
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
 *
 * input:
 * kafka-console-producer.sh --broker-list hadoop103:9092 --topic user-info
 * >1,wz,27,male
 * >2,xiaol,25,female
 * >3,huage,38,male
 * >4,kanghao,24,male
 */
public class Consumer实现ExactlyOnce手段1 {
    public static void main(String[] args) throws IOException, SQLException {
        // 从配置文件中加载写的参数
        Properties props = new Properties();
        props.load(ConsumerDemo3.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-3");
        // 关闭自动位移提交机制
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 创建jdbc连接(conn对象默认没执行一条语句都是一个事务，都会做一次自动提交)
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/abc?useSSL=false",
                "root", "hadoop");
        // 关闭jdbc默认的事务自动提交
        conn.setAutoCommit(false);

        // 业务数据sql
        PreparedStatement pstData = conn.prepareStatement("insert into stu_info values (?, ?, ?, ?)");
        // 更新偏移量sql: 利用mysql自身的幂等性 (设表的主键是 topic_partition，利用幂等性语法可减少开发量)
        // 不使用mysql幂等性来维护偏移量也可以，就是代码量大、效率比较低
        PreparedStatement pstOffset = conn.prepareStatement("INSERT INTO t_offsets VALUES(?, ?) ON DUPLICATE KEY UPDATE offset = ?");
        // 查询分区偏移量sql
        PreparedStatement pstQueryOffset = conn.prepareStatement("select offset from t_offsets where topic_partition = ?");
        // 订阅主题, 触发分区再均衡时(启动当前消费者进程 或者 后面组内加入新的消费者等)，对当前消费者负责的分区进行位移的初始化
        // id,name,age,gender
        consumer.subscribe(Arrays.asList("user-info"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 用于收拾残局
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 我重新分配了哪些分区，我要从DB中获取这些分区的最新位移，然后我进行seek
                try {
                    for (TopicPartition topicPartition : partitions) {
                        pstQueryOffset.setString(1, topicPartition.topic() + ":" + topicPartition.partition());
                        ResultSet resultSet = pstQueryOffset.executeQuery();
                        resultSet.next();
                        long offset = resultSet.getLong("offset");
                        System.out.println("发生分区再均衡，该消费组获得分区" + topicPartition + ", 最新消费位移是" + offset);
                        consumer.seek(topicPartition, offset);
                        // 如果我得到的是一个新分区，我seek其为0，起始应该也不用：后面我肯定会消费它，对于这个topic而言，
                        // 新的分区进入新数据，后面被当前消费者接着消费即可
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            // 遍历拉取到的一批数据
            for (ConsumerRecord<String, String> record: records) {
                try {
                    String data = record.value();
                    // 解析原始数据
                    String[] fields = data.split(",");

                    // 设置业务sql param
                    pstData.setInt(1, Integer.parseInt(fields[0]));
                    pstData.setString(2, fields[1]);
                    pstData.setInt(3, Integer.parseInt(fields[2]));
                    pstData.setString(4, fields[3]);

                    // 执行业务数据插入，在事务中，未提交
                    pstData.execute();

                    // 测试事务的回滚
                    //if (fields[0].equals("4")) {
                    //    throw new Exception("hhhhh~ occurs exception after insert");
                    //}

                    // 设置偏移量sql param
                    pstOffset.setString(1, record.topic() + ":" + record.partition());
                    // kafka下一次读取的那条记录，其偏移量是当前这条消息的偏移量 + 1（kafka的分区偏移量从0开始）
                    pstOffset.setLong(2, record.offset() + 1);
                    pstOffset.setLong(3, record.offset() + 1);

                    // 更新偏移量到mysql，在事务中，未提交
                    pstOffset.execute();
                    /** 极端场景：当要更新偏移量时，发生了分区再均衡，那么消费者进程则无法继续执行pstOffset.execute()语句了
                     * 此时消费的分区位移没有被更新，如果该分区被分配给其他消费者，则又要重新插入一下该条数据，
                     *
                     * 不过，当前消费者没有提交事务，因此mysql后面会自动回滚事务，因此别的消费者再插入一条数据也是满足精确一致性的
                     *
                     * 如果有些中间件不支持自动回滚呢？ 可以在onPartitionsRevoked中进行 残局处理
                     */


                    // 提交jdbc事务，下一次再来就是新事务
                    conn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    // 回滚事务
                    conn.rollback();

                }
            }
        }

        pstData.close();
        conn.close();
        consumer.close();
    }
}
