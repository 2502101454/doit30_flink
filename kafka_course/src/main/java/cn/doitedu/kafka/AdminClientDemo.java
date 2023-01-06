package cn.doitedu.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author zengwang
 * @create 2022-12-06 20:21
 * @desc: 练习kafka Topic的API
 */
public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        // 管理客户端
        AdminClient adminClient = KafkaAdminClient.create(props);
        // 创建一个topic
        //NewTopic zzuzz = new NewTopic("zzuzz", 3, (short) 2);
        // 创建延迟好大，什么鬼
        //CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(zzuzz));

        // 查看topic的详细信息
        DescribeTopicsResult topicDescriptions = adminClient.describeTopics(Arrays.asList("zzuzz"));

        KafkaFuture<Map<String, TopicDescription>> descriptions = topicDescriptions.all();
        Map<String, TopicDescription> infos = descriptions.get();
        Set<Map.Entry<String, TopicDescription>> entries = infos.entrySet();
        for (Map.Entry<String, TopicDescription> entry : entries) {
            String topicName = entry.getKey();

            TopicDescription td = entry.getValue();
            List<TopicPartitionInfo> partitions = td.partitions();
            for (TopicPartitionInfo partition : partitions) {
                int partitionIndex = partition.partition();
                Node leader = partition.leader();
                List<Node> replicas = partition.replicas();
                List<Node> isr = partition.isr();

                System.out.println(String.format("Topic: %s\tpartition: %d\tleader: %s\treplicas: %s\tisr: %s",
                        topicName, partitionIndex, leader, replicas, isr));
            }
        }

        adminClient.close();
    }
}

class TestMain {
    public static void connectKf() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        // 管理客户端
        AdminClient adminClient = KafkaAdminClient.create(props);
        // 创建一个topic
        NewTopic zzuzz = new NewTopic("zzuzz", 3, (short) 2);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(zzuzz));
    }
}