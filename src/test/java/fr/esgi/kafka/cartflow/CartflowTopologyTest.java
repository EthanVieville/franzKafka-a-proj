package fr.esgi.kafka.cartflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.DlqMessage;
import fr.esgi.kafka.cartflow.model.OrderConfirmed;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests de topologie CART-1/CART-2. Outil : TopologyTestDriver (deja en
 * scope test, s'execute a chaque "mvn package" via le plugin Surefire,
 * avant le packaging). On teste la Topology nue construite via
 * StreamsBuilder, sans demarrer Spring ni broker.
 */
class CartflowTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> ordersIn;
    private TestInputTopic<String, String> paymentsIn;
    private TestInputTopic<String, String> stockIn;
    private TestOutputTopic<String, String> dlqOut;
    private TestOutputTopic<String, OrderConfirmed> confirmedOut;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new CartflowTopology().pipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cartflow-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);

        ordersIn = driver.createInputTopic(
                Topics.ORDERS, new StringSerializer(), new StringSerializer());
        paymentsIn = driver.createInputTopic(
                Topics.PAYMENTS, new StringSerializer(), new StringSerializer());
        stockIn = driver.createInputTopic(
                Topics.STOCK_MOVEMENTS, new StringSerializer(), new StringSerializer());
        dlqOut = driver.createOutputTopic(
                Topics.DLQ, new StringDeserializer(), new StringDeserializer());
        confirmedOut = driver.createOutputTopic(
                Topics.ORDERS_CONFIRMED, new StringDeserializer(), JsonSerdes.of(OrderConfirmed.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    // ------------------------------------------------------------------
    // Messages invalides -> DLQ (un cas par type d'anomalie du README)
    // ------------------------------------------------------------------

    @Test
    void commandeSansOrderIdPartEnDlq() throws Exception {
        // Champ requis absent.
        String invalidJson = """
                {"user_id": "u-0327", "card_id": "card-0057", \
                "items": [{"sku": "sku-0000", "product": "Clavier", \
                "category": "HIGH-TECH", "qty": 1, "unit_price": 10.0}], \
                "total": 10.0, "currency": "EUR", "shipping_city": "Paris", \
                "timestamp": "2026-07-04T14:03:21.512Z"}""";

        ordersIn.pipeInput("card-0057", invalidJson);

        DlqMessage dlq = readSingleDlqMessage("card-0057");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.ORDERS);
        assertThat(dlq.reason()).isEqualTo("order_id manquant");
        assertThat(dlq.originalMessage()).isEqualTo(invalidJson);
    }

    @Test
    void jsonTronqueSurOrdersPartEnDlqSansCrash() throws Exception {
        // Poison pill : JSON tronque. L'application ne doit jamais crasher.
        String truncatedJson = "{\"order_id\": \"or";

        ordersIn.pipeInput("card-0099", truncatedJson);

        DlqMessage dlq = readSingleDlqMessage("card-0099");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.ORDERS);
        assertThat(dlq.reason()).isEqualTo("JSON illisible ou tronque");
        assertThat(dlq.originalMessage()).isEqualTo(truncatedJson);
    }

    @Test
    void commandeAvecTimestampIllisiblePartEnDlq() throws Exception {
        String invalidJson = """
                {"order_id": "ord-1", "user_id": "u-1", "card_id": "card-01", \
                "items": [{"sku": "sku-1", "product": "Clavier", \
                "category": "HIGH-TECH", "qty": 1, "unit_price": 10.0}], \
                "total": 10.0, "currency": "EUR", "shipping_city": "Paris", \
                "timestamp": "hier a 15h"}""";

        ordersIn.pipeInput("card-01", invalidJson);

        DlqMessage dlq = readSingleDlqMessage("card-01");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.ORDERS);
        assertThat(dlq.reason()).isEqualTo("timestamp illisible");
    }

    @Test
    void paiementAvecStatusInconnuPartEnDlq() throws Exception {
        // Enum inconnue.
        String invalidJson = """
                {"payment_id": "pay-1", "order_id": "ord-1", "card_id": "card-01", \
                "amount": 10.0, "status": "AUTHORIZEDD", \
                "timestamp": "2026-07-04T14:05:47.020Z"}""";

        paymentsIn.pipeInput("card-01", invalidJson);

        DlqMessage dlq = readSingleDlqMessage("card-01");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.PAYMENTS);
        assertThat(dlq.reason()).isEqualTo("status inconnu: AUTHORIZEDD");
    }

    @Test
    void paiementAvecMontantNegatifPartEnDlq() throws Exception {
        // Valeur hors bornes.
        String invalidJson = """
                {"payment_id": "pay-1", "order_id": "ord-1", "card_id": "card-01", \
                "amount": -50.0, "status": "AUTHORIZED", \
                "timestamp": "2026-07-04T14:05:47.020Z"}""";

        paymentsIn.pipeInput("card-01", invalidJson);

        DlqMessage dlq = readSingleDlqMessage("card-01");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.PAYMENTS);
        assertThat(dlq.reason()).isEqualTo("amount manquant ou <= 0");
    }

    @Test
    void mouvementStockSansWarehousePartEnDlqSansCrash() throws Exception {
        // Champ requis absent (warehouse null) : cas qui provoquait un NPE
        // (Set.of(...).contains(null)) avant correction de Validators.
        String invalidJson = """
                {"sku": "sku-1", "delta": -1, \
                "timestamp": "2026-07-04T14:03:21.512Z"}""";

        stockIn.pipeInput("sku-1", invalidJson);

        DlqMessage dlq = readSingleDlqMessage("sku-1");
        assertThat(dlq.sourceTopic()).isEqualTo(Topics.STOCK_MOVEMENTS);
        assertThat(dlq.reason()).isEqualTo("warehouse inconnu: null");
    }

    // ------------------------------------------------------------------
    // Messages valides -> ne partent PAS en DLQ
    // ------------------------------------------------------------------

    @Test
    void commandeValideNePartPasEnDlq() {
        String validJson = """
                {"order_id": "ord-1", "user_id": "u-1", "card_id": "card-01", \
                "items": [{"sku": "sku-1", "product": "Clavier", \
                "category": "HIGH-TECH", "qty": 1, "unit_price": 10.0}], \
                "total": 10.0, "currency": "EUR", "shipping_city": "Paris", \
                "timestamp": "2026-07-04T14:03:21.512Z"}""";

        ordersIn.pipeInput("card-01", validJson);

        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void paiementValideNePartPasEnDlq() {
        String validJson = """
                {"payment_id": "pay-1", "order_id": "ord-1", "card_id": "card-01", \
                "amount": 10.0, "status": "AUTHORIZED", \
                "timestamp": "2026-07-04T14:05:47.020Z"}""";

        paymentsIn.pipeInput("card-01", validJson);

        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void mouvementStockValideNePartPasEnDlq() {
        String validJson = """
                {"sku": "sku-1", "delta": -1, "warehouse": "ORLY-1", \
                "timestamp": "2026-07-04T14:03:21.512Z"}""";

        stockIn.pipeInput("sku-1", validJson);

        assertThat(dlqOut.isEmpty()).isTrue();
    }

    // ------------------------------------------------------------------
    // CART-2 - join orders x payments (fenetre 10 min, AUTHORIZED only)
    // ------------------------------------------------------------------

    @Test
    void commandePayeeSousDixMinEstConfirmee() {
        Instant orderTime = Instant.parse("2026-07-04T14:00:00.000Z");
        ordersIn.pipeInput("card-01", orderJson("ord-42", "card-01"), orderTime);
        paymentsIn.pipeInput("card-01", paymentJson("pay-42", "ord-42", "card-01", "AUTHORIZED"),
                orderTime.plus(Duration.ofMinutes(2)));

        assertThat(confirmedOut.isEmpty()).isFalse();
        KeyValue<String, OrderConfirmed> kv = confirmedOut.readKeyValue();
        assertThat(kv.key).isEqualTo("ord-42");
        assertThat(kv.value.order().orderId()).isEqualTo("ord-42");
        assertThat(kv.value.payment().paymentId()).isEqualTo("pay-42");
        assertThat(confirmedOut.isEmpty()).isTrue();
    }

    @Test
    void commandeJamaisPayeeNeRessortJamais() {
        Instant orderTime = Instant.parse("2026-07-04T14:00:00.000Z");
        ordersIn.pipeInput("card-02", orderJson("ord-99", "card-02"), orderTime);

        assertThat(confirmedOut.isEmpty()).isTrue();
    }

    @Test
    void paiementHorsFenetreDeDixMinNeConfirmePas() {
        Instant orderTime = Instant.parse("2026-07-04T14:00:00.000Z");
        ordersIn.pipeInput("card-03", orderJson("ord-11", "card-03"), orderTime);
        // 11 min apres : hors de la fenetre de 10 min (JoinWindows.ofTimeDifferenceWithNoGrace).
        paymentsIn.pipeInput("card-03", paymentJson("pay-11", "ord-11", "card-03", "AUTHORIZED"),
                orderTime.plus(Duration.ofMinutes(11)));

        assertThat(confirmedOut.isEmpty()).isTrue();
    }

    @Test
    void paiementDeclineNeConfirmePasLaCommande() {
        Instant orderTime = Instant.parse("2026-07-04T14:00:00.000Z");
        ordersIn.pipeInput("card-04", orderJson("ord-77", "card-04"), orderTime);
        paymentsIn.pipeInput("card-04", paymentJson("pay-77", "ord-77", "card-04", "DECLINED"),
                orderTime.plus(Duration.ofMinutes(1)));

        assertThat(confirmedOut.isEmpty()).isTrue();
    }

    private static String orderJson(String orderId, String cardId) {
        return """
                {"order_id": "%s", "user_id": "u-1", "card_id": "%s", \
                "items": [{"sku": "sku-1", "product": "Clavier", \
                "category": "HIGH-TECH", "qty": 1, "unit_price": 10.0}], \
                "total": 10.0, "currency": "EUR", "shipping_city": "Paris", \
                "timestamp": "2026-07-04T14:00:00.000Z"}""".formatted(orderId, cardId);
    }

    private static String paymentJson(String paymentId, String orderId, String cardId, String status) {
        return """
                {"payment_id": "%s", "order_id": "%s", "card_id": "%s", \
                "amount": 10.0, "status": "%s", \
                "timestamp": "2026-07-04T14:02:00.000Z"}""".formatted(paymentId, orderId, cardId, status);
    }

    private DlqMessage readSingleDlqMessage(String expectedKey) throws Exception {
        assertThat(dlqOut.isEmpty()).isFalse();
        KeyValue<String, String> record = dlqOut.readKeyValue();
        assertThat(record.key).isEqualTo(expectedKey);
        assertThat(dlqOut.isEmpty()).isTrue();
        return MAPPER.readValue(record.value, DlqMessage.class);
    }
}
