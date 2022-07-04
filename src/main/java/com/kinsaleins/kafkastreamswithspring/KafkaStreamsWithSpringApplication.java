package com.kinsaleins.kafkastreamswithspring;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class KafkaStreamsWithSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsWithSpringApplication.class, args);
    }

}

