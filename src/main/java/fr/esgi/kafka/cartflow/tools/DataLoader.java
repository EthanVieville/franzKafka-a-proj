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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Outil de développement hors du contexte Spring.
 *
 * Modes disponibles :
 *
 * 1. Envoi immédiat de toutes les données :
 *    mvn compile exec:java
 *
 * 2. Envoi immédiat avec une limite par fichier :
 *    mvn compile exec:java -Dexec.args="--limit 500"
 *
 * 3. Envoi en streaming sur les trois topics :
 *    mvn compile exec:java \
 *      -Dexec.args="--batch-size 10 --interval-ms 1000 --live-timestamps"
 *
 * 4. Création des topics uniquement :
 *    mvn compile exec:java -Dexec.args="--topics-only"
 */
public final class DataLoader {

    /**
     * Liste ordonnée des fichiers et de leurs topics.
     *
     * En mode streaming, un batch sera envoyé sur chaque source
     * dans cet ordre avant la pause.
     */
    private static final List<SourceDefinition> SOURCES = List.of(
            new SourceDefinition(
                    Path.of("data/raw/cartflow.orders.jsonl"),
                    Topics.ORDERS
            ),
            new SourceDefinition(
                    Path.of("data/raw/cartflow.payments.jsonl"),
                    Topics.PAYMENTS
            ),
            new SourceDefinition(
                    Path.of("data/raw/cartflow.stock.movements.jsonl"),
                    Topics.STOCK_MOVEMENTS
            )
    );

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 3;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DataLoader() {
        // Classe utilitaire non instanciable.
    }

    public static void main(String[] args) throws Exception {
        LoaderOptions options = parseOptions(args);

        String bootstrap = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP",
                        "localhost:29092"
                );

        printConfiguration(bootstrap, options);

        createMissingTopics(bootstrap);

        if (options.topicsOnly()) {
            System.out.println(
                    "Creation des topics terminee. Aucun message envoye."
            );
            return;
        }

        Properties producerProperties =
                createProducerProperties(bootstrap);

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(producerProperties)) {

            if (options.batchSize() > 0) {
                replayStreaming(producer, options);
            } else {
                replayImmediate(producer, options);
            }
        }

        System.out.println("Chargement des donnees termine.");
    }

    private static void printConfiguration(
            String bootstrap,
            LoaderOptions options
    ) {
        System.out.println("Bootstrap Kafka : " + bootstrap);
        System.out.println("Groupe           : " + Topics.GROUPE);
        System.out.println("DLQ              : " + Topics.DLQ);

        if (options.topicsOnly()) {
            System.out.println("Mode             : creation des topics");
            return;
        }

        if (options.batchSize() > 0) {
            System.out.println("Mode             : streaming");
            System.out.println(
                    "Batch/topic       : " + options.batchSize()
            );
            System.out.println(
                    "Intervalle        : "
                            + options.intervalMs()
                            + " ms"
            );
            System.out.println(
                    "Timestamps live   : "
                            + options.liveTimestamps()
            );
        } else if (options.limit() != null) {
            System.out.println("Mode             : envoi limite");
        } else {
            System.out.println("Mode             : envoi complet");
        }

        System.out.println(
                "Limite/fichier    : "
                        + (options.limit() == null
                        ? "aucune"
                        : options.limit())
        );
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
        Properties adminProperties = new Properties();

        adminProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap
        );

        Set<String> wantedTopics = new HashSet<>();

        for (SourceDefinition source : SOURCES) {
            wantedTopics.add(source.topic());
        }

        wantedTopics.add(Topics.DLQ);

        try (AdminClient admin =
                     AdminClient.create(adminProperties)) {

            Set<String> existingTopics =
                    admin.listTopics().names().get();

            List<NewTopic> missingTopics = wantedTopics.stream()
                    .filter(topic ->
                            !existingTopics.contains(topic))
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
     * Mode normal ou limité.
     *
     * Les fichiers sont traités successivement, sans pause.
     */
    private static void replayImmediate(
            KafkaProducer<String, String> producer,
            LoaderOptions options
    ) throws IOException {

        for (SourceDefinition source : SOURCES) {
            long count = replayImmediateSource(
                    producer,
                    source,
                    options
            );

            System.out.printf(
                    "%s -> %s : %d messages envoyes%n",
                    source.file(),
                    source.topic(),
                    count
            );
        }
    }

    private static long replayImmediateSource(
            KafkaProducer<String, String> producer,
            SourceDefinition source,
            LoaderOptions options
    ) throws IOException {

        checkFileExists(source.file());

        long totalSent = 0;

        try (BufferedReader reader =
                     Files.newBufferedReader(source.file())) {

            String line;

            while ((line = nextNonBlankLine(reader)) != null) {
                if (options.limit() != null
                        && totalSent >= options.limit()) {
                    break;
                }

                sendEnvelope(
                        producer,
                        source.topic(),
                        line,
                        options.liveTimestamps()
                );

                totalSent++;
            }
        }

        producer.flush();

        return totalSent;
    }

    /**
     * Mode streaming alterné sur les trois topics.
     *
     * Exemple avec batchSize=10 :
     *
     * - 10 messages dans cartflow.orders ;
     * - 10 messages dans cartflow.payments ;
     * - 10 messages dans cartflow.stock.movements ;
     * - flush ;
     * - pause ;
     * - cycle suivant.
     *
     * La limite est appliquée séparément à chaque fichier.
     */
    private static void replayStreaming(
            KafkaProducer<String, String> producer,
            LoaderOptions options
    ) throws IOException, InterruptedException {

        List<SourceCursor> cursors = openSourceCursors();

        long cycle = 0;

        try {
            while (hasActiveSource(cursors)) {
                cycle++;

                int totalCycleSent = 0;

                System.out.printf(
                        "%n--- Cycle streaming %d ---%n",
                        cycle
                );

                for (SourceCursor cursor : cursors) {
                    int batchSent = sendBatch(
                            producer,
                            cursor,
                            options
                    );

                    totalCycleSent += batchSent;

                    System.out.printf(
                            "%s : %d messages envoyes"
                                    + " (total : %d)%n",
                            cursor.source().topic(),
                            batchSent,
                            cursor.totalSent()
                    );
                }

                producer.flush();

                if (totalCycleSent == 0) {
                    break;
                }

                if (hasActiveSource(cursors)
                        && options.intervalMs() > 0) {

                    System.out.printf(
                            "Pause de %d ms...%n",
                            options.intervalMs()
                    );

                    Thread.sleep(options.intervalMs());
                }
            }
        } finally {
            closeSourceCursors(cursors);
        }

        System.out.println();
        System.out.println("Resume du streaming :");

        for (SourceCursor cursor : cursors) {
            System.out.printf(
                    "%s -> %s : %d messages envoyes%n",
                    cursor.source().file(),
                    cursor.source().topic(),
                    cursor.totalSent()
            );
        }
    }

    private static List<SourceCursor> openSourceCursors()
            throws IOException {

        List<SourceCursor> cursors = new ArrayList<>();

        try {
            for (SourceDefinition source : SOURCES) {
                checkFileExists(source.file());

                cursors.add(
                        new SourceCursor(
                                source,
                                Files.newBufferedReader(source.file())
                        )
                );
            }

            return cursors;
        } catch (IOException exception) {
            closeSourceCursors(cursors);
            throw exception;
        }
    }

    private static void closeSourceCursors(
            List<SourceCursor> cursors
    ) {
        for (SourceCursor cursor : cursors) {
            try {
                cursor.close();
            } catch (IOException exception) {
                System.err.println(
                        "Impossible de fermer le fichier "
                                + cursor.source().file()
                                + " : "
                                + exception.getMessage()
                );
            }
        }
    }

    private static boolean hasActiveSource(
            List<SourceCursor> cursors
    ) {
        return cursors.stream()
                .anyMatch(SourceCursor::isActive);
    }

    private static int sendBatch(
            KafkaProducer<String, String> producer,
            SourceCursor cursor,
            LoaderOptions options
    ) throws IOException {

        if (!cursor.isActive()) {
            return 0;
        }

        int batchSent = 0;

        while (batchSent < options.batchSize()) {
            if (options.limit() != null
                    && cursor.totalSent() >= options.limit()) {

                cursor.finish();
                break;
            }

            String line =
                    nextNonBlankLine(cursor.reader());

            if (line == null) {
                cursor.finish();
                break;
            }

            sendEnvelope(
                    producer,
                    cursor.source().topic(),
                    line,
                    options.liveTimestamps()
            );

            cursor.incrementTotalSent();
            batchSent++;
        }

        return batchSent;
    }

    private static void sendEnvelope(
            KafkaProducer<String, String> producer,
            String topic,
            String line,
            boolean liveTimestamps
    ) throws IOException {

        RawEnvelope envelope =
                MAPPER.readValue(
                        line,
                        RawEnvelope.class
                );

        long timestamp = liveTimestamps
                ? System.currentTimeMillis()
                : envelope.tsMs();

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        topic,
                        null,
                        timestamp,
                        envelope.key(),
                        envelope.value()
                );

        producer.send(record);
    }

    private static String nextNonBlankLine(
            BufferedReader reader
    ) throws IOException {

        String line;

        while ((line = reader.readLine()) != null) {
            if (!line.isBlank()) {
                return line;
            }
        }

        return null;
    }

    private static void checkFileExists(
            Path file
    ) throws IOException {

        if (!Files.exists(file)) {
            throw new IOException(
                    "Fichier introuvable : "
                            + file.toAbsolutePath()
            );
        }
    }

    private static LoaderOptions parseOptions(
            String[] args
    ) {
        Integer limit = null;
        int batchSize = 0;
        long intervalMs = 0;
        boolean liveTimestamps = false;
        boolean topicsOnly = false;

        for (int i = 0; i < args.length; i++) {
            String argument = args[i];

            switch (argument) {
                case "--limit" -> {
                    limit = parseNonNegativeInteger(
                            args,
                            ++i,
                            "--limit"
                    );
                }

                case "--batch-size" -> {
                    batchSize = parsePositiveInteger(
                            args,
                            ++i,
                            "--batch-size"
                    );
                }

                case "--interval-ms" -> {
                    intervalMs = parseNonNegativeLong(
                            args,
                            ++i,
                            "--interval-ms"
                    );
                }

                case "--live-timestamps" ->
                        liveTimestamps = true;

                case "--topics-only" ->
                        topicsOnly = true;

                default -> throw new IllegalArgumentException(
                        "Argument inconnu : " + argument
                );
            }
        }

        if (intervalMs > 0 && batchSize == 0) {
            throw new IllegalArgumentException(
                    "--interval-ms necessite --batch-size."
            );
        }

        return new LoaderOptions(
                limit,
                batchSize,
                intervalMs,
                liveTimestamps,
                topicsOnly
        );
    }

    private static int parsePositiveInteger(
            String[] args,
            int index,
            String option
    ) {
        int value = parseNonNegativeInteger(
                args,
                index,
                option
        );

        if (value == 0) {
            throw new IllegalArgumentException(
                    option + " doit etre superieur a 0."
            );
        }

        return value;
    }

    private static int parseNonNegativeInteger(
            String[] args,
            int index,
            String option
    ) {
        String rawValue = getOptionValue(
                args,
                index,
                option
        );

        int value;

        try {
            value = Integer.parseInt(rawValue);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Valeur invalide pour "
                            + option
                            + " : "
                            + rawValue,
                    exception
            );
        }

        if (value < 0) {
            throw new IllegalArgumentException(
                    option
                            + " doit etre superieur"
                            + " ou egal a 0."
            );
        }

        return value;
    }

    private static long parseNonNegativeLong(
            String[] args,
            int index,
            String option
    ) {
        String rawValue = getOptionValue(
                args,
                index,
                option
        );

        long value;

        try {
            value = Long.parseLong(rawValue);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Valeur invalide pour "
                            + option
                            + " : "
                            + rawValue,
                    exception
            );
        }

        if (value < 0) {
            throw new IllegalArgumentException(
                    option
                            + " doit etre superieur"
                            + " ou egal a 0."
            );
        }

        return value;
    }

    private static String getOptionValue(
            String[] args,
            int index,
            String option
    ) {
        if (index >= args.length) {
            throw new IllegalArgumentException(
                    "L'option "
                            + option
                            + " attend une valeur."
            );
        }

        return args[index];
    }

    private record LoaderOptions(
            Integer limit,
            int batchSize,
            long intervalMs,
            boolean liveTimestamps,
            boolean topicsOnly
    ) {
    }

    private record SourceDefinition(
            Path file,
            String topic
    ) {
    }

    private static final class SourceCursor
            implements AutoCloseable {

        private final SourceDefinition source;
        private final BufferedReader reader;

        private long totalSent;
        private boolean active = true;

        private SourceCursor(
                SourceDefinition source,
                BufferedReader reader
        ) {
            this.source = source;
            this.reader = reader;
        }

        SourceDefinition source() {
            return source;
        }

        BufferedReader reader() {
            return reader;
        }

        long totalSent() {
            return totalSent;
        }

        boolean isActive() {
            return active;
        }

        void incrementTotalSent() {
            totalSent++;
        }

        void finish() {
            active = false;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
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