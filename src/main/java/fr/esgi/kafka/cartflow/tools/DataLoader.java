package fr.esgi.kafka.cartflow.tools;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.cartflow.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Outil de dev (PAS partie de la topologie / hors scan Spring) : rejoue les
 * echantillons data/raw/*.jsonl sur le cluster Kafka local, sur les topics
 * source d'entree (Topics.ORDERS/PAYMENTS/STOCK_MOVEMENTS - seule source de
 * verite pour les noms). Cree les topics s'ils n'existent pas encore.
 *
 * Usage : mvn exec:java
 *         mvn exec:java -Dexec.args="--limit 500"
 */
public final class DataLoader {

    private static final Map<String, String> FILE_TO_TOPIC = Map.of(
            "data/raw/cartflow.orders.jsonl", Topics.ORDERS,
            "data/raw/cartflow.payments.jsonl", Topics.PAYMENTS,
            "data/raw/cartflow.stock.movements.jsonl", Topics.STOCK_MOVEMENTS);

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 3;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DataLoader() {
    }

    public static void main(String[] args) throws Exception {
        int limit = parseLimit(args);
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:29092");

        createMissingTopics(bootstrap);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (Map.Entry<String, String> entry : FILE_TO_TOPIC.entrySet()) {
                long count = replay(producer, Path.of(entry.getKey()), entry.getValue(), limit);
                System.out.printf("%s -> %s : %d messages envoyes%n", entry.getKey(), entry.getValue(), count);
            }
        }
    }

    private static void createMissingTopics(String bootstrap) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = FILE_TO_TOPIC.values().stream()
                    .distinct()
                    .filter(topic -> !existing.contains(topic))
                    .map(topic -> new NewTopic(topic, PARTITIONS, REPLICATION_FACTOR))
                    .toList();

            if (missing.isEmpty()) {
                return;
            }
            admin.createTopics(missing).all().get();
            missing.forEach(topic -> System.out.println("Topic cree : " + topic.name()));
        }
    }

    /** Une ligne source = {"key": ..., "value": "<json ou texte brut>", "ts_ms": ...}. */
    private static long replay(KafkaProducer<String, String> producer, Path file, String topic, int limit)
            throws IOException {
        long count = 0;
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null && (limit <= 0 || count < limit)) {
                if (line.isBlank()) {
                    continue;
                }
                RawEnvelope envelope = MAPPER.readValue(line, RawEnvelope.class);
                producer.send(new ProducerRecord<>(topic, null, envelope.tsMs(), envelope.key(), envelope.value()));
                count++;
            }
        }
        producer.flush();
        return count;
    }

    private static int parseLimit(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--limit".equals(args[i])) {
                return Integer.parseInt(args[i + 1]);
            }
        }
        return -1;
    }

    private record RawEnvelope(
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("ts_ms") long tsMs) {
    }
}
