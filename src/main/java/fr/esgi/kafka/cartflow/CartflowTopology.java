package fr.esgi.kafka.cartflow;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Construisez ici la topologie du projet CARTFLOW.
 * Spring Boot demarre Kafka Streams automatiquement grace a
 * {@code @EnableKafkaStreams} + la config spring.kafka.streams.* de
 * application.yml. Chaque ticket du backlog (README) correspond a un
 * bloc ci-dessous.
 */
@Configuration
@EnableKafkaStreams
public class CartflowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(CartflowTopology.class);

    @Bean
    public KStream<String, String> pipeline(StreamsBuilder builder) {

        KStream<String, String> rawOrders = builder.stream(
                Topics.ORDERS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawPayments = builder.stream(
                Topics.PAYMENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawStock = builder.stream(
                Topics.STOCK_MOVEMENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawOrders.peek((key, value) -> LOG.info("[cartflow] {} -> {}", key, value));

        // -----------------------------------------------------------------
        // CART-1 - Ingestion fiable (les TROIS flux)
        //   Parser (model.Order / Payment / StockMovement), valider, router
        //   les invalides vers Topics.DLQ avec message original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // CART-2 - Commandes confirmees (join orders x payments, fenetre 10 min)
        //                                              -> Topics.ORDERS_CONFIRMED
        // CART-3 - CA par categorie (tumbling 1 min)   -> Topics.REVENUE_BY_CATEGORY
        // CART-4 - Detection de fraude carte           -> Topics.ALERTS_FRAUD
        // CART-5 - Stock courant + alerte rupture      -> Topics.STOCK_LEVELS
        //                                                 et Topics.ALERTS_STOCK
        // CART-6 (bonus) - exactly_once_v2 + demonstration (cf. README)

        return rawOrders;
    }
}
