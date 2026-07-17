package fr.esgi.kafka.cartflow;


import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.FraudAlert;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CartflowFraudTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> ordersIn;
    private TestInputTopic<String, String> paymentsIn;
    private TestOutputTopic<String, FraudAlert> fraudOut;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new CartflowTopology().pipeline(builder);

        Properties properties = new Properties();
        properties.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "cartflow-fraud-test"
        );
        properties.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "dummy:1234"
        );

        driver = new TopologyTestDriver(
                builder.build(),
                properties
        );

        ordersIn = driver.createInputTopic(
                Topics.ORDERS,
                new StringSerializer(),
                new StringSerializer()
        );

        paymentsIn = driver.createInputTopic(
                Topics.PAYMENTS,
                new StringSerializer(),
                new StringSerializer()
        );

        fraudOut = driver.createOutputTopic(
                Topics.ALERTS_FRAUD,
                new StringDeserializer(),
                JsonSerdes.of(FraudAlert.class)
                        .deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void detecteTroisCommandesDansDeuxVillesEnCinqMinutes() {
        Instant start =
                Instant.parse("2026-07-16T14:00:00Z");

        confirmOrder(
                "ord-1",
                "card-fraud",
                "Paris",
                start
        );

        confirmOrder(
                "ord-2",
                "card-fraud",
                "Rennes",
                start.plusSeconds(30)
        );

        confirmOrder(
                "ord-3",
                "card-fraud",
                "Lyon",
                start.plusSeconds(90)
        );

        assertThat(fraudOut.isEmpty()).isFalse();

        List<FraudAlert> alerts =
                fraudOut.readValuesToList();

        FraudAlert lastAlert =
                alerts.get(alerts.size() - 1);

        assertThat(lastAlert.cardId())
                .isEqualTo("card-fraud");

        assertThat(lastAlert.orderCount())
                .isGreaterThanOrEqualTo(3);

        assertThat(lastAlert.cities())
                .contains("Paris", "Rennes", "Lyon");
    }

    @Test
    void carteFideleMonoVilleNeDeclenchePasAlerte() {
        Instant start =
                Instant.parse("2026-07-16T14:00:00Z");

        confirmOrder(
                "ord-1",
                "card-loyal",
                "Paris",
                start
        );

        confirmOrder(
                "ord-2",
                "card-loyal",
                "Paris",
                start.plusSeconds(30)
        );

        confirmOrder(
                "ord-3",
                "card-loyal",
                "Paris",
                start.plusSeconds(60)
        );

        confirmOrder(
                "ord-4",
                "card-loyal",
                "Paris",
                start.plusSeconds(90)
        );

        assertThat(fraudOut.isEmpty()).isTrue();
    }

    @Test
    void commandesTropEloigneesNeDeclenchentPasAlerte() {
        Instant start =
                Instant.parse("2026-07-16T14:00:00Z");

        confirmOrder(
                "ord-1",
                "card-slow",
                "Paris",
                start
        );

        confirmOrder(
                "ord-2",
                "card-slow",
                "Rennes",
                start.plus(Duration.ofMinutes(4))
        );

        confirmOrder(
                "ord-3",
                "card-slow",
                "Lyon",
                start.plus(Duration.ofMinutes(8))
        );

        assertThat(fraudOut.isEmpty()).isTrue();
    }

    private void confirmOrder(
            String orderId,
            String cardId,
            String city,
            Instant time
    ) {
        ordersIn.pipeInput(
                cardId,
                orderJson(orderId, cardId, city),
                time
        );

        paymentsIn.pipeInput(
                cardId,
                paymentJson(
                        "pay-" + orderId,
                        orderId,
                        cardId
                ),
                time.plusSeconds(5)
        );
    }

    private static String orderJson(
            String orderId,
            String cardId,
            String city
    ) {
        return """
                {
                  "order_id": "%s",
                  "user_id": "u-1",
                  "card_id": "%s",
                  "items": [
                    {
                      "sku": "sku-1",
                      "product": "Clavier",
                      "category": "HIGH-TECH",
                      "qty": 1,
                      "unit_price": 10.0
                    }
                  ],
                  "total": 10.0,
                  "currency": "EUR",
                  "shipping_city": "%s",
                  "timestamp": "2026-07-16T14:00:00Z"
                }
                """.formatted(
                orderId,
                cardId,
                city
        );
    }

    private static String paymentJson(
            String paymentId,
            String orderId,
            String cardId
    ) {
        return """
                {
                  "payment_id": "%s",
                  "order_id": "%s",
                  "card_id": "%s",
                  "amount": 10.0,
                  "status": "AUTHORIZED",
                  "timestamp": "2026-07-16T14:00:05Z"
                }
                """.formatted(
                paymentId,
                orderId,
                cardId
        );
    }
}