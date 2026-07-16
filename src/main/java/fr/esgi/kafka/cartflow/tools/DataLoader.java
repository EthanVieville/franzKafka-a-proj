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
 * Charge les fichiers JSONL dans Kafka.
 *
 * <p>Modes :
 * <ul>
 *   <li>tout envoyer : {@code mvn compile exec:java}</li>
 *   <li>limiter par fichier : {@code -Dexec.args="--limit 500"}</li>
 *   <li>streaming alterné :
 *       {@code -Dexec.args="--batch-size 10 --interval-ms 1000 --live-timestamps"}</li>
 *   <li>créer les topics : {@code -Dexec.args="--topics-only"}</li>
 * </ul>
 */
public final class DataLoader {

    private static final List<Source> SOURCES = List.of(
            new Source(Path.of("data/raw/cartflow.orders.jsonl"), Topics.ORDERS),
            new Source(Path.of("data/raw/cartflow.payments.jsonl"), Topics.PAYMENTS),
            new Source(Path.of("data/raw/cartflow.stock.movements.jsonl"), Topics.STOCK_MOVEMENTS)
    );

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 3;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DataLoader() {
    }

    public static void main(String[] args) throws Exception {
        Options options = parseOptions(args);
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:29092");

        printConfiguration(bootstrap, options);
        createMissingTopics(bootstrap);

        if (options.topicsOnly()) {
            System.out.println("Creation des topics terminee. Aucun message envoye.");
            return;
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(bootstrap))) {
            if (options.streaming()) {
                replayStreaming(producer, options);
            } else {
                replayImmediate(producer, options);
            }
        }

        System.out.println("Chargement des donnees termine.");
    }

    private static void printConfiguration(String bootstrap, Options options) {
        String mode = options.topicsOnly() ? "creation des topics"
                : options.streaming() ? "streaming"
                : options.limit() != null ? "envoi limite"
                : "envoi complet";

        System.out.printf("Bootstrap Kafka : %s%nGroupe           : %s%nDLQ              : %s%nMode             : %s%n",
                bootstrap, Topics.GROUPE, Topics.DLQ, mode);

        if (!options.topicsOnly()) {
            System.out.printf("Limite/fichier   : %s%n",
                    options.limit() == null ? "aucune" : options.limit());
        }
        if (options.streaming()) {
            System.out.printf("Batch/topic      : %d%nIntervalle       : %d ms%nTimestamps live  : %s%n",
                    options.batchSize(), options.intervalMs(), options.liveTimestamps());
        }
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void createMissingTopics(String bootstrap) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        Set<String> wanted = new HashSet<>();
        SOURCES.forEach(source -> wanted.add(source.topic()));
        wanted.add(Topics.DLQ);

        try (AdminClient admin = AdminClient.create(properties)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = wanted.stream()
                    .filter(topic -> !existing.contains(topic))
                    .map(topic -> new NewTopic(topic, PARTITIONS, REPLICATION_FACTOR))
                    .toList();

            if (missing.isEmpty()) {
                System.out.println("Tous les topics existent deja.");
                return;
            }

            admin.createTopics(missing).all().get();
            missing.forEach(topic -> System.out.println("Topic cree : " + topic.name()));
        }
    }

    /** Envoi immédiat, avec une limite facultative appliquée à chaque fichier. */
    private static void replayImmediate(KafkaProducer<String, String> producer, Options options)
            throws IOException {
        for (Source source : SOURCES) {
            checkFile(source.file());
            long sent = 0;

            try (BufferedReader reader = Files.newBufferedReader(source.file())) {
                String line;
                while ((line = nextLine(reader)) != null
                        && (options.limit() == null || sent < options.limit())) {
                    send(producer, source.topic(), line, options.liveTimestamps());
                    sent++;
                }
            }

            producer.flush();
            System.out.printf("%s -> %s : %d messages envoyes%n",
                    source.file(), source.topic(), sent);
        }
    }

    /**
     * Envoi round-robin : un batch sur chaque topic, un flush, une pause,
     * puis un nouveau cycle jusqu'à épuisement des trois fichiers.
     */
    private static void replayStreaming(KafkaProducer<String, String> producer, Options options)
            throws IOException, InterruptedException {
        List<Cursor> cursors = openCursors();
        long cycle = 0;

        try {
            while (cursors.stream().anyMatch(Cursor::active)) {
                System.out.printf("%n--- Cycle streaming %d ---%n", ++cycle);
                int cycleSent = 0;

                for (Cursor cursor : cursors) {
                    int sent = sendBatch(producer, cursor, options);
                    cycleSent += sent;
                    System.out.printf("%s : %d messages (total : %d)%n",
                            cursor.source().topic(), sent, cursor.total());
                }

                producer.flush();
                if (cycleSent == 0) {
                    break;
                }
                if (cursors.stream().anyMatch(Cursor::active) && options.intervalMs() > 0) {
                    Thread.sleep(options.intervalMs());
                }
            }
        } finally {
            closeCursors(cursors);
        }

        System.out.println("\nResume du streaming :");
        cursors.forEach(cursor -> System.out.printf("%s -> %s : %d messages envoyes%n",
                cursor.source().file(), cursor.source().topic(), cursor.total()));
    }

    private static int sendBatch(KafkaProducer<String, String> producer, Cursor cursor, Options options)
            throws IOException {
        int sent = 0;

        while (cursor.active() && sent < options.batchSize()) {
            if (options.limit() != null && cursor.total() >= options.limit()) {
                cursor.finish();
                break;
            }

            String line = nextLine(cursor.reader());
            if (line == null) {
                cursor.finish();
                break;
            }

            send(producer, cursor.source().topic(), line, options.liveTimestamps());
            cursor.increment();
            sent++;

            if (options.limit() != null && cursor.total() >= options.limit()) {
                cursor.finish();
            }
        }
        return sent;
    }

    private static void send(KafkaProducer<String, String> producer, String topic, String line,
                             boolean liveTimestamps) throws IOException {
        RawEnvelope envelope = MAPPER.readValue(line, RawEnvelope.class);
        long timestamp = liveTimestamps ? System.currentTimeMillis() : envelope.tsMs();
        producer.send(new ProducerRecord<>(topic, null, timestamp, envelope.key(), envelope.value()));
    }

    private static String nextLine(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.isBlank()) {
                return line;
            }
        }
        return null;
    }

    private static List<Cursor> openCursors() throws IOException {
        List<Cursor> cursors = new ArrayList<>();
        try {
            for (Source source : SOURCES) {
                checkFile(source.file());
                cursors.add(new Cursor(source, Files.newBufferedReader(source.file())));
            }
            return cursors;
        } catch (IOException exception) {
            closeCursors(cursors);
            throw exception;
        }
    }

    private static void closeCursors(List<Cursor> cursors) {
        for (Cursor cursor : cursors) {
            try {
                cursor.close();
            } catch (IOException exception) {
                System.err.printf("Impossible de fermer %s : %s%n",
                        cursor.source().file(), exception.getMessage());
            }
        }
    }

    private static void checkFile(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("Fichier introuvable : " + file.toAbsolutePath());
        }
    }

    private static Options parseOptions(String[] args) {
        Integer limit = null;
        int batchSize = 0;
        long intervalMs = 0;
        boolean liveTimestamps = false;
        boolean topicsOnly = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--limit" -> limit = parseInt(value(args, ++i, "--limit"), "--limit", true);
                case "--batch-size" -> batchSize = parseInt(value(args, ++i, "--batch-size"), "--batch-size", false);
                case "--interval-ms" -> intervalMs = parseLong(value(args, ++i, "--interval-ms"), "--interval-ms");
                case "--live-timestamps" -> liveTimestamps = true;
                case "--topics-only" -> topicsOnly = true;
                default -> throw new IllegalArgumentException("Argument inconnu : " + args[i]);
            }
        }

        if (intervalMs > 0 && batchSize == 0) {
            throw new IllegalArgumentException("--interval-ms necessite --batch-size.");
        }
        return new Options(limit, batchSize, intervalMs, liveTimestamps, topicsOnly);
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " attend une valeur.");
        }
        return args[index];
    }

    private static int parseInt(String raw, String option, boolean zeroAllowed) {
        try {
            int value = Integer.parseInt(raw);
            if (value < 0 || (!zeroAllowed && value == 0)) {
                throw new IllegalArgumentException(option + (zeroAllowed
                        ? " doit etre superieur ou egal a 0."
                        : " doit etre superieur a 0."));
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Valeur invalide pour " + option + " : " + raw, exception);
        }
    }

    private static long parseLong(String raw, String option) {
        try {
            long value = Long.parseLong(raw);
            if (value < 0) {
                throw new IllegalArgumentException(option + " doit etre superieur ou egal a 0.");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Valeur invalide pour " + option + " : " + raw, exception);
        }
    }

    private record Options(Integer limit, int batchSize, long intervalMs,
                           boolean liveTimestamps, boolean topicsOnly) {
        boolean streaming() {
            return batchSize > 0;
        }
    }

    private record Source(Path file, String topic) {
    }

    private static final class Cursor implements AutoCloseable {
        private final Source source;
        private final BufferedReader reader;
        private long total;
        private boolean active = true;

        private Cursor(Source source, BufferedReader reader) {
            this.source = source;
            this.reader = reader;
        }

        Source source() { return source; }
        BufferedReader reader() { return reader; }
        long total() { return total; }
        boolean active() { return active; }
        void increment() { total++; }
        void finish() { active = false; }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private record RawEnvelope(
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("ts_ms") long tsMs
    ) {
    }
}
