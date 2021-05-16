package com.example.kafka.util;

import com.alibaba.fastjson.JSONObject;
import com.example.kafka.api.Result;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

import static com.example.kafka.util.consumerUtil.getConsumer;

public class topicUtil {

    public static AdminClient createAdminClientByProperties(String brokers) {
        Properties prop = new Properties();
        prop.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        prop.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
        prop.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "2000");
        return AdminClient.create(prop);
    }

    /**
     * 查询kafka集群 （broker id,host,端口）
     *
     * @param broker
     * @return
     */
    public static Result clusterInfo(String broker) {
        AdminClient client = null;
        try {
            client = createAdminClientByProperties(broker);
            DescribeClusterResult describeClusterResult = client.describeCluster();
            Node controller = describeClusterResult.controller().get();
            Collection<Node> nodes = describeClusterResult.nodes().get();

            //转为json格式
            List<JSONObject> collect = nodes.stream().map(node -> {
                JSONObject jo = new JSONObject();
                jo.put("host", node.host());
                jo.put("port", node.port());
                jo.put("idStr", node.idString());
                jo.put("id", node.id());
                if (node.id() == controller.id()) {
                    jo.put("controller", true);
                } else {
                    jo.put("controller", false);
                }
                return jo;
            }).collect(Collectors.toList());

            return Result.success(collect);
        } catch (Exception e) {
            return Result.failed(e.getMessage());
        } finally {
            client.close();
        }

    }

    /**
     * 查询topic
     *
     * @param brokers
     * @return
     */
    public static Result listTopicsWithOptions(String brokers) {
        AdminClient adminClient = null;
        try {
            // 创建AdminClient客户端对象
            adminClient = createAdminClientByProperties(brokers);
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true);

            // 列出所有的topic
            ListTopicsResult result = adminClient.listTopics(options);
            Collection<TopicListing> topicListings = result.listings().get();

            Result success = Result.success(topicListings);
            return success;
        } catch (Exception e) {
            return Result.failed(e.getMessage());
        } finally {
            adminClient.close();
        }
    }

    /**
     * 创建topic
     *
     * @param brokers
     * @param topic
     * @param partition
     * @param replica
     * @throws Exception
     */
    public static void createTopic(String brokers, String topic, Integer partition, Integer replica) throws Exception {
        AdminClient adminClient = null;
        try {
            adminClient = createAdminClientByProperties(brokers);
            List<NewTopic> topicList = new ArrayList();
            NewTopic newTopic = new NewTopic(topic, partition, replica.shortValue());
            topicList.add(newTopic);
            CreateTopicsResult result = adminClient.createTopics(topicList);
            result.all().get();
            result.values().forEach((name, future) -> System.out.println("topicName:" + name));
        } catch (Exception e) {
        } finally {
            adminClient.close();
        }
    }

    /**
     * 查询topic分区详情（分区号，leader分区，所有副本，isr副本，最小偏移量，最大偏移量）
     *
     * @param broker
     * @param topic
     * @return
     * @throws Exception
     */
    public static JSONObject getTopicDetail(String broker, String topic) throws Exception {
        // 返回的json对象
        JSONObject res = new JSONObject();

        AdminClient adminClient = createAdminClientByProperties(broker);
        List<String> list = new ArrayList<>();
        list.add(topic);
        DescribeTopicsResult result = adminClient.describeTopics(list);
        Map<String, TopicDescription> map = result.all().get();
        //查询topic信息
        TopicDescription topicDescription = map.get(topic);

        res.put("isInternal", topicDescription.isInternal());//否是内部主题
        res.put("name", topicDescription.name());//主题名字

        //分区
        List<TopicPartition> topicPartitions = topicDescription.partitions().stream().map(t -> {
            TopicPartition topicPartition = new TopicPartition(topic, t.partition());
            return topicPartition;
        }).collect(Collectors.toList());
        //偏移量
        KafkaConsumer<String, String> consumer = getConsumer(broker, topic, "Kafka", "earliest");
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);

        List<JSONObject> collect = topicDescription.partitions().stream().map(t -> {
            JSONObject p = new JSONObject();

            Node leader = t.leader();
            List<JSONObject> replicas = t.replicas().stream().map(r -> node2Json(r)).collect(Collectors.toList());
            List<JSONObject> isr = t.isr().stream().map(r -> node2Json(r)).collect(Collectors.toList());
            Long endOffset = endOffsets.get(new TopicPartition(topic, t.partition()));
            Long beginningOffset = beginningOffsets.get(new TopicPartition(topic, t.partition()));

            p.put("partition", t.partition());//分区号
            p.put("leader", node2Json(leader));//leader分区
            p.put("replicas", replicas);//replicas
            p.put("isr", isr);//isr replicas
            p.put("endOffset", endOffset);//最小偏移量
            p.put("beginningOffset", beginningOffset);//最大偏移量

            return p;
        }).collect(Collectors.toList());
        res.put("partitions", collect);
        adminClient.close();
        return res;
    }

    /**
     * 删除topic
     *
     * @param broker
     * @param name
     */
    public static void deleteTopic(String broker, String name) {
        AdminClient adminClient = createAdminClientByProperties(broker);
        List<String> list = new ArrayList<>();
        list.add(name);
        adminClient.deleteTopics(list);
        adminClient.close();
    }


    public static JSONObject node2Json(Node node) {
        JSONObject leaderNode = new JSONObject();
        leaderNode.put("id", node.id());
        leaderNode.put("host", node.host());
        leaderNode.put("port", node.port());
        leaderNode.put("rack", node.rack());
        return leaderNode;
    }
}
