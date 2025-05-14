package app.kafka;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Profile({"consume","produce-consume"})
public class TestConsumer {
    TestConsumer(){
        System.out.println("--------------------------");
        System.out.println("Consumer started...");
        System.out.println("--------------------------");
    }
    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.group-id}")
    public void listen(String message) {
        System.out.println("Consumed: " + message);
    }
}

