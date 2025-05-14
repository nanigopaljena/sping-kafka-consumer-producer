package app.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Profile({"produce","produce-consume"})
public class TestProducer {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${spring.kafka.topic-name}")
    private String topicName;

    @Value("${spring.kafka.group-id}")
    private String groupId;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private int counter = 0;

    TestProducer(){
        System.out.println("------------------------------------------------");
        System.out.println("Will wait for 5 seconds before producing...");
        System.out.println("------------------------------------------------");
    }

    @Scheduled(initialDelay = 5000, fixedRate = 1900)
    public void sendMessage() {

        if (counter == 10) {
            System.out.println("Produced 10 messages, stopping producer.");
        }
        if (counter >= 10) {
            counter++;
            return;
        }
        

        String message = "Message number: #" + counter++;
        kafkaTemplate.send(topicName, message);
        if(counter==1){
            System.out.println("------------------------------");
            System.out.println("Producing started at: " + new Date());
            System.out.println("-------------------------------");
        }
        System.out.println("Produced: " + message);
    }
}
