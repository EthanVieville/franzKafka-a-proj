package fr.esgi.kafka.cartflow;

import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.CategoryRevenue;
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

import org.assertj.core.api.InstanceOfAssertFactories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests CART-3 (CA par categorie, TimeWindows tumbling 1 min).
 * Source : commandes brutes valides (CART-1), pas les commandes confirmees
 * de CART-2 - choix assume, voir CartflowTopology.
 */
class CartflowRevenueTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> ordersIn;
    private TestOutputTopic<String, CategoryRevenue> revenueOut;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new CartflowTopology().pipeline(builder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "cartflow-revenue-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put("statestore.cache.max.bytes", 0);

        driver = new TopologyTestDriver(builder.build(), properties);

        ordersIn = driver.createInputTopic(
                Topics.ORDERS, new StringSerializer(), new StringSerializer());
        revenueOut = driver.createOutputTopic(
                Topics.REVENUE_BY_CATEGORY,
                new StringDeserializer(),
                JsonSerdes.of(CategoryRevenue.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void eclateUneCommandeMultiCategoriesEnPlusieursLignes() {
        Instant t = Instant.parse("2026-07-16T14:00:10Z");
        order("ord-1", "card-1", t,
                item("sku-1", "HIGH-TECH", 2, 100.0),
                item("sku-2", "MAISON", 1, 50.0));

        List<CategoryRevenue> revenues = revenueOut.readValuesToList();

        assertThat(revenues).hasSize(2);
        assertThat(revenues).filteredOn(r -> r.category().equals("HIGH-TECH"))
                .singleElement()
                .extracting(CategoryRevenue::revenue, InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(200.0, within(0.01));
        assertThat(revenues).filteredOn(r -> r.category().equals("MAISON"))
                .singleElement()
                .extracting(CategoryRevenue::revenue, InstanceOfAssertFactories.DOUBLE)
                .isCloseTo(50.0, within(0.01));
    }

    @Test
    void sommeDeuxCommandesDeMemeCategorieDansLaMemeFenetre() {
        Instant t = Instant.parse("2026-07-16T14:00:00Z");
        order("ord-1", "card-1", t,
                item("sku-1", "HIGH-TECH", 1, 100.0));
        order("ord-2", "card-2", t.plusSeconds(30),
                item("sku-1", "HIGH-TECH", 1, 50.0));

        List<CategoryRevenue> revenues = revenueOut.readValuesToList();
        CategoryRevenue last = revenues.get(revenues.size() - 1);

        assertThat(last.category()).isEqualTo("HIGH-TECH");
        assertThat(last.revenue()).isCloseTo(150.0, within(0.01));
    }

    @Test
    void neSommePasDeuxCommandesDeFenetresDifferentes() {
        Instant t = Instant.parse("2026-07-16T14:00:00Z");
        order("ord-1", "card-1", t,
                item("sku-1", "HIGH-TECH", 1, 100.0));
        order("ord-2", "card-2", t.plus(Duration.ofMinutes(2)),
                item("sku-1", "HIGH-TECH", 1, 50.0));

        List<CategoryRevenue> revenues = revenueOut.readValuesToList();

        assertThat(revenues).hasSize(2);
        assertThat(revenues).extracting(CategoryRevenue::revenue)
                .containsExactly(100.0, 50.0);
        assertThat(revenues.get(0).windowStart()).isNotEqualTo(revenues.get(1).windowStart());
    }

    @Test
    void commandeInvalideNeContribuePasAuChiffreDAffaires() {
        String invalidJson = """
                {"order_id": "ord-1", "user_id": "u-1", "card_id": "card-1", \
                "items": [{"sku": "sku-1", "product": "Clavier", \
                "category": "HIGH-TECH", "qty": 1, "unit_price": 10.0}], \
                "total": -10.0, "currency": "EUR", "shipping_city": "Paris", \
                "timestamp": "2026-07-16T14:00:00Z"}""";

        ordersIn.pipeInput("card-1", invalidJson, Instant.parse("2026-07-16T14:00:00Z"));

        assertThat(revenueOut.isEmpty()).isTrue();
    }

    private void order(String orderId, String cardId, Instant time, Item... items) {
        StringBuilder itemsJson = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                itemsJson.append(",");
            }
            itemsJson.append("""
                    {"sku": "%s", "product": "Produit", "category": "%s", \
                    "qty": %d, "unit_price": %s}\
                    """.formatted(items[i].sku, items[i].category, items[i].qty, items[i].unitPrice));
        }

        String json = """
                {"order_id": "%s", "user_id": "u-1", "card_id": "%s", \
                "items": [%s], "total": 999.0, "currency": "EUR", \
                "shipping_city": "Paris", "timestamp": "%s"}\
                """.formatted(orderId, cardId, itemsJson, time);

        ordersIn.pipeInput(cardId, json, time);
    }

    private static Item item(String sku, String category, int qty, double unitPrice) {
        return new Item(sku, category, qty, unitPrice);
    }

    private record Item(String sku, String category, int qty, double unitPrice) {
    }
}
