package fr.esgi.kafka.cartflow;

import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.StockAlert;
import fr.esgi.kafka.cartflow.model.StockLevel;
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

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CartflowStockTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> stockIn;
    private TestOutputTopic<String, StockLevel> levelsOut;
    private TestOutputTopic<String, StockAlert> alertsOut;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new CartflowTopology().pipeline(builder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "cartflow-stock-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put("statestore.cache.max.bytes", 0);

        driver = new TopologyTestDriver(builder.build(), properties);
        stockIn = driver.createInputTopic(
                Topics.STOCK_MOVEMENTS, new StringSerializer(), new StringSerializer());
        levelsOut = driver.createOutputTopic(
                Topics.STOCK_LEVELS,
                new StringDeserializer(),
                JsonSerdes.of(StockLevel.class).deserializer());
        alertsOut = driver.createOutputTopic(
                Topics.ALERTS_STOCK,
                new StringDeserializer(),
                JsonSerdes.of(StockAlert.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void cumuleLesDeltasEnPartantDeCent() {
        move("sku-1", -20, 0);
        move("sku-1", -15, 1);
        move("sku-1", 10, 2);

        List<StockLevel> levels = levelsOut.readValuesToList();
        assertThat(levels).extracting(StockLevel::level)
                .containsExactly(80, 65, 75);
        assertThat(alertsOut.isEmpty()).isTrue();
    }

    @Test
    void alerteUniquementLorsDuPassageSousDix() {
        move("sku-1", -90, 0); // 100 -> 10 : pas sous 10
        move("sku-1", -1, 1);  // 10 -> 9  : alerte
        move("sku-1", -2, 2);  // 9 -> 7   : pas de nouvelle alerte

        List<StockAlert> alerts = alertsOut.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.getFirst().sku()).isEqualTo("sku-1");
        assertThat(alerts.getFirst().level()).isEqualTo(9);
        assertThat(alerts.getFirst().threshold()).isEqualTo(10);
    }

    @Test
    void reassortFaitRemonterLeStockEtPermetUneNouvelleAlerte() {
        move("sku-1", -95, 0); // 5 : première alerte
        move("sku-1", 20, 1);  // 25 : réassort
        move("sku-1", -20, 2); // 5 : nouvelle rupture

        assertThat(levelsOut.readValuesToList())
                .extracting(StockLevel::level)
                .containsExactly(5, 25, 5);
        assertThat(alertsOut.readValuesToList()).hasSize(2);
    }

    @Test
    void maintientUnEtatIndependantPourChaqueSku() {
        move("sku-1", -20, 0);
        move("sku-2", -5, 1);

        assertThat(levelsOut.readKeyValuesToMap())
                .satisfies(levels -> {
                    assertThat(levels.get("sku-1").level()).isEqualTo(80);
                    assertThat(levels.get("sku-2").level()).isEqualTo(95);
                });
    }

    private void move(String sku, int delta, long seconds) {
        String json = """
                {"sku":"%s","delta":%d,"warehouse":"ORLY-1",
                 "timestamp":"2026-07-16T14:00:00Z"}
                """.formatted(sku, delta);
        stockIn.pipeInput(sku, json,
                Instant.parse("2026-07-16T14:00:00Z").plusSeconds(seconds));
    }
}