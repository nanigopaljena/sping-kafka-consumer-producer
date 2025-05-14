package app.kafka;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.Date;

@Service
@Profile({"consume","produce-consume"})
public class TestConsumer {
    private int consumeCount = 0;
    

    TestConsumer(){
        System.out.println("--------------------------");
        System.out.println("Consumer started...");
        System.out.println("--------------------------");
    }
    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.group-id}")
    public void listen(String message) {
        System.out.println("Consumed: " + message);
        consumeCount++;

        if (consumeCount >= 10) {
            System.out.println("------------------------------");
            System.out.println("Consumed 10 messages, exiting at: " + new Date());
            System.out.println("------------------------------");
            System.exit(0); // Forcefully terminate the app
        }
    }
}

