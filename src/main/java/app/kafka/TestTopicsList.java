package app.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
@Profile("list-topics")
public class TestTopicsList {
    TestTopicsList(){
        System.out.println("--------------------------");
        System.out.println("Topic list started...");
        System.out.println("--------------------------");
    }
    @Autowired
    private AdminClient adminClient;

    @Value("${spring.kafka.print-topic-config:false}")
    private boolean printTopicConfig;


    @Scheduled(initialDelay = 1000, fixedRate = Long.MAX_VALUE)
    public void listAllTopics() {
        try {
            if(printTopicConfig){
                System.out.println("Printing topics with config details...");
                printTopicDetails();
            }else{
                ListTopicsOptions options = new ListTopicsOptions();
                options.listInternal(false);
                Set<String> topicNames = adminClient.listTopics(options).names().get();
                if(topicNames.isEmpty()){
                    System.err.println("No topics found");
                }else{
                    System.out.println("------------------------------------------------");
                    System.out.println("TOPIC LIST");
                    System.out.println("------------------------------------------------");
                    for (String topic : topicNames) {
                        System.out.println(topic);
                    }
                    System.out.println("------------------------------------------------");
                }
            }
        } catch (Exception e) {
            System.err.println("No topics found "+ e.getMessage());
            e.printStackTrace();
        }
        System.err.println("Shutting down after listing topics...");
        System.exit(0);
    }


    public void printTopicDetails() throws ExecutionException, InterruptedException {
        // Step 1: Get topic names
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false); // exclude internal topics
        Set<String> topicNames = adminClient.listTopics(options).names().get();

        // Step 2: Describe topics to get partition count
        DescribeTopicsResult topicDescriptions = adminClient.describeTopics(topicNames);
        Map<String, TopicDescription> descriptionMap = topicDescriptions.all().get();

        // Step 3: Describe configs to get topic configurations
        Collection<ConfigResource> configResources = new ArrayList<>();
        for (String topic : topicNames) {
            configResources.add(new ConfigResource(ConfigResource.Type.TOPIC, topic));
        }

        DescribeConfigsResult configsResult = adminClient.describeConfigs(configResources);
        Map<ConfigResource, Config> configMap = configsResult.all().get();

        // Step 4: Print partition counts and configurations
        for (String topic : topicNames) {
            TopicDescription desc = descriptionMap.get(topic);
            System.out.println("Topic: " + topic + " Partitions: " + desc.partitions().size());
            Config config = configMap.get(new ConfigResource(ConfigResource.Type.TOPIC, topic));
            System.out.println("Configs:");
            for (ConfigEntry entry : config.entries()) {
                System.out.println("    " + entry.name() + " = " + entry.value());
            }
            System.out.println("------------------------------------------------");
        }
    }

    @Bean
    public AdminClient kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}

