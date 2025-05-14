package app.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaApp {
    public static void main(String[] args) {
        SpringApplication.run(KafkaApp.class, args);
        System.out.println("--------------------------");
        System.out.println("Application started...");
        System.out.println("--------------------------");

    }
}
