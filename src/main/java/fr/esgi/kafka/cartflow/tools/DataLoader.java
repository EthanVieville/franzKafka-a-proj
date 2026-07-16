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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Outil de développement, hors du contexte Spring.
 *
 * Il permet :
 * - de créer les topics sources et le DLQ ;
 * - de rejouer les fichiers data/raw/*.jsonl ;
 * - de limiter le nombre de messages envoyés ;
 * - de créer uniquement les topics avec --topics-only.
 *
 * Exemples :
 *
 * mvn compile exec:java
 * mvn compile exec:java -Dexec.args="--limit 500"
 * mvn compile exec:java -Dexec.args="--limit 0"
 * mvn compile exec:java -Dexec.args="--topics-only"
 */
public final class DataLoader {

    private static final Map<String, String> FILE_TO_TOPIC = Map.of(
            "data/raw/cartflow.orders.jsonl",
            Topics.ORDERS,

            "data/raw/cartflow.payments.jsonl",
            Topics.PAYMENTS,

            "data/raw/cartflow.stock.movements.jsonl",
            Topics.STOCK_MOVEMENTS
    );

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 3;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DataLoader() {
        // Classe utilitaire non instanciable.
    }

    public static void main(String[] args) throws Exception {
        boolean topicsOnly = List.of(args).contains("--topics-only");

        String bootstrap = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP",
                        "localhost:29092"
                );

        System.out.println("Bootstrap Kafka : " + bootstrap);
        System.out.println("Groupe           : " + Topics.GROUPE);
        System.out.println("DLQ              : " + Topics.DLQ);

        createMissingTopics(bootstrap);

        if (topicsOnly) {
            System.out.println(
                    "Creation des topics terminee. Aucun message envoye."
            );
            return;
        }

        /*
         * null signifie qu'aucune limite n'est appliquée.
         *
         * Il faut conserver le type Integer et non int, car parseLimit()
         * peut retourner null.
         */
        Integer limit = parseLimit(args);

        Properties producerProps = createProducerProperties(bootstrap);

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(producerProps)) {

            for (Map.Entry<String, String> entry
                    : FILE_TO_TOPIC.entrySet()) {

                Path file = Path.of(entry.getKey());
                String topic = entry.getValue();

                long count = replay(
                        producer,
                        file,
                        topic,
                        limit
                );

                System.out.printf(
                        "%s -> %s : %d messages envoyes%n",
                        file,
                        topic,
                        count
                );
            }
        }

        System.out.println("Chargement des donnees termine.");
    }

    private static Properties createProducerProperties(
            String bootstrap
    ) {
        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        return properties;
    }

    private static void createMissingTopics(
            String bootstrap
    ) throws Exception {
        Properties adminProps = new Properties();

        adminProps.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap
        );

        /*
         * Création des trois topics sources et du DLQ.
         *
         * Le DLQ est créé explicitement pour ne pas dépendre de
         * l'auto-création Kafka au premier message invalide.
         */
        Set<String> wantedTopics =
                new HashSet<>(FILE_TO_TOPIC.values());

        wantedTopics.add(Topics.DLQ);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existingTopics =
                    admin.listTopics().names().get();

            List<NewTopic> missingTopics = wantedTopics.stream()
                    .filter(topic -> !existingTopics.contains(topic))
                    .map(topic -> new NewTopic(
                            topic,
                            PARTITIONS,
                            REPLICATION_FACTOR
                    ))
                    .toList();

            if (missingTopics.isEmpty()) {
                System.out.println(
                        "Tous les topics existent deja."
                );
                return;
            }

            admin.createTopics(missingTopics)
                    .all()
                    .get();

            missingTopics.forEach(topic ->
                    System.out.println(
                            "Topic cree : " + topic.name()
                    )
            );
        }
    }

    /**
     * Format d'une ligne source :
     *
     * {
     *   "key": "...",
     *   "value": "<json ou texte brut>",
     *   "ts_ms": 123456789
     * }
     */
    private static long replay(
            KafkaProducer<String, String> producer,
            Path file,
            String topic,
            Integer limit
    ) throws IOException {
        if (!Files.exists(file)) {
            throw new IOException(
                    "Fichier introuvable : "
                            + file.toAbsolutePath()
            );
        }

        long count = 0;

        try (BufferedReader reader =
                     Files.newBufferedReader(file)) {

            String line;

            while ((limit == null || count < limit)
                    && (line = reader.readLine()) != null) {

                if (line.isBlank()) {
                    continue;
                }

                RawEnvelope envelope =
                        MAPPER.readValue(
                                line,
                                RawEnvelope.class
                        );

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                topic,
                                null,
                                envelope.tsMs(),
                                envelope.key(),
                                envelope.value()
                        );

                producer.send(record);
                count++;
            }
        }

        producer.flush();

        return count;
    }

    /**
     * Interprétation de l'option --limit :
     *
     * - absence de --limit : null, donc tout envoyer ;
     * - --limit 500       : envoyer au maximum 500 messages ;
     * - --limit 0         : ne transmettre aucun message.
     */
    private static Integer parseLimit(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (!"--limit".equals(args[i])) {
                continue;
            }

            if (i + 1 >= args.length) {
                throw new IllegalArgumentException(
                        "L'option --limit attend une valeur entiere."
                );
            }

            int limit;

            try {
                limit = Integer.parseInt(args[i + 1]);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(
                        "Valeur invalide pour --limit : "
                                + args[i + 1],
                        exception
                );
            }

            if (limit < 0) {
                throw new IllegalArgumentException(
                        "--limit doit etre superieur ou egal a 0."
                );
            }

            return limit;
        }

        return null;
    }

    private record RawEnvelope(
            @JsonProperty("key")
            String key,

            @JsonProperty("value")
            String value,

            @JsonProperty("ts_ms")
            long tsMs
    ) {
    }
}