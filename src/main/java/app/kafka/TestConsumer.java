package app.kafka;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.Date;
import java.util.concurrent.*;

@Service
@Profile({"consume","produce-consume"})
public class TestConsumer {
    private int consumeCount = 0;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> shutdownTask;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${spring.kafka.topic-name}")
    private String topicName;

    @Value("${spring.kafka.group-id}")
    private String groupId;


    TestConsumer(){
        System.out.println("--------------------------");
        System.out.println("Consumer started...");
        System.out.println("--------------------------");
    }
    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.group-id}")
    public void listen(String message) {

        System.out.println("Bootstrap server: " + bootstrapServer);
        System.out.println("Topic name: " + groupId);
        System.out.println("GroupId name: " + groupId);

        System.out.println("Consumed: " + message);
        scheduleShutdown();
    }

    private void scheduleShutdown() {
        if (shutdownTask != null && !shutdownTask.isDone()) {
            shutdownTask.cancel(false);
        }

        // Schedule the shutdown to occur after 10 seconds of inactivity
        shutdownTask = scheduler.schedule(() -> {
            System.out.println("------------------------------");
            System.out.println("Exiting after 10 seconds of inactivity at: " + new Date());
            System.out.println("------------------------------");
            System.exit(0);  // Forcefully exit the application
        }, 10, TimeUnit.SECONDS);
    }
}

