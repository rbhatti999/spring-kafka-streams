package com.kinsaleins.kafkastreamswithspring;

import com.kinsaleins.avro.POCEntity;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.properties.input.topic}")
    String inputTopic;

    @Value("${spring.kafka.properties.output.topic}")
    String outputTopic;

    @Value("${spring.kafka.properties.bootstrap.servers}")
    String bootstrapServers;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    String saslJaasConfig;

    @Value("${spring.kafka.properties.security.protocol}")
    String securityProtocol;

    @Value("${spring.kafka.properties.basic.auth.credentials.source}")
    String credentialsSource;

    @Value("${spring.kafka.properties.basic.auth.user.info}")
    String userInfo;

    @Value("${spring.kafka.properties.schema.registry.url}")
    String schemaRegistryURL;

    private static final Logger logger = LogManager.getLogger(KafkaStreamsConfig.class);

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        return new KafkaStreamsConfiguration(Map.of(
            APPLICATION_ID_CONFIG, "spring-kafka-streams",
            BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            SASL_MECHANISM, saslMechanism,
            SASL_JAAS_CONFIG, saslJaasConfig,
            SECURITY_PROTOCOL_CONFIG, securityProtocol,
            "basic.auth.credentials.source", credentialsSource,
            "schema.registry.basic.auth.user.info", userInfo,
            "schema.registry.url", schemaRegistryURL,
            DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class
        ));
    }
    @Bean
    public KStream<String, POCEntity> kStream(StreamsBuilder builder) {
        KStream<String, POCEntity> firstStream = builder.stream(inputTopic);

        KStream<String, POCEntity> transformStream = firstStream
            .peek((key, pocEntity) -> logger.log(Level.INFO, "Event Record: " + pocEntity))
            .filter((key, pocEntity) -> pocEntity.getNmeEventType().toString().equals("ReadyToSubmit"))
            .mapValues(pocEntity -> POCEntity.newBuilder()
                .setIdtEnterpriseTxnEntity(pocEntity.getIdtEnterpriseTxnEntity().toString())
                .setNumEnterpriseTxnEntityEdition(pocEntity.getNumEnterpriseTxnEntityEdition())
                .setNumEnterpriseTxnEntityVersion(pocEntity.getNumEnterpriseTxnEntityVersion())
                .setDteEventOccurred(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT))
                .setNmeCreator("StreamsApp")
                .setNmeEventType("NewSubmission").build())
            .peek((key, pocEntity) -> logger.log(Level.INFO, "Transformed Record: " + pocEntity));

        transformStream
            .peek((key, pocEntity) -> logger.log(Level.INFO, "Posting Transformed Record to Kafka"))
            .to(outputTopic);

        transformStream
            .peek((key, pocEntity) -> logger.log(Level.INFO, "Successfully Posted Record to Kafka"));

        return firstStream;

    }
}
