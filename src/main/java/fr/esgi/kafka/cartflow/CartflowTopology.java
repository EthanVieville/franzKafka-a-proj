package fr.esgi.kafka.cartflow;

import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.DlqMessage;
import fr.esgi.kafka.cartflow.model.Order;
import fr.esgi.kafka.cartflow.model.Payment;
import fr.esgi.kafka.cartflow.model.StockMovement;
import fr.esgi.kafka.cartflow.validation.ValidationResult;
import fr.esgi.kafka.cartflow.validation.Validators;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Map;
import java.util.function.Function;

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

    @Bean
    public KStream<String, Order> pipeline(StreamsBuilder builder) {

        KStream<String, String> rawOrders = builder.stream(
                Topics.ORDERS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawPayments = builder.stream(
                Topics.PAYMENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawStock = builder.stream(
                Topics.STOCK_MOVEMENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // CART-1 - Ingestion fiable (les TROIS flux)
        //   Parse + valide chaque flux, route les invalides vers Topics.DLQ
        //   (message original + raison). JsonSerdes.parseOrNull protege du
        //   poison pill ; validateAndRouteToDlq() mutualise le split/branch
        //   commun aux 3 flux, la logique de validation reste par type
        //   (Validators.validateOrder/validatePayment/validateStockMovement).
        // -----------------------------------------------------------------
        KStream<String, Order> validOrders =
                validateAndRouteToDlq(rawOrders, Topics.ORDERS, Validators::validateOrder);
        KStream<String, Payment> validPayments =
                validateAndRouteToDlq(rawPayments, Topics.PAYMENTS, Validators::validatePayment);
        KStream<String, StockMovement> validStock =
                validateAndRouteToDlq(rawStock, Topics.STOCK_MOVEMENTS, Validators::validateStockMovement);

        // CART-2 - Commandes confirmees (join orders x payments, fenetre 10 min)
        //                                              -> Topics.ORDERS_CONFIRMED
        // CART-3 - CA par categorie (tumbling 1 min)   -> Topics.REVENUE_BY_CATEGORY
        // CART-4 - Detection de fraude carte           -> Topics.ALERTS_FRAUD
        // CART-5 - Stock courant + alerte rupture      -> Topics.STOCK_LEVELS
        //                                                 et Topics.ALERTS_STOCK
        // CART-6 (bonus) - exactly_once_v2 + demonstration (cf. README)

        return validOrders;
    }

    /**
     * Parse + valide un flux brut ; route les messages invalides vers la DLQ
     * (topic source + raison + JSON original) et ne laisse passer que les
     * messages valides, deserialises dans leur type metier.
     */
    private <T> KStream<String, T> validateAndRouteToDlq(
            KStream<String, String> rawStream,
            String sourceTopic,
            Function<String, ValidationResult<T>> validator) {

        KStream<String, ValidationResult<T>> checked = rawStream.mapValues(validator::apply);

        Map<String, KStream<String, ValidationResult<T>>> branches = checked
                .split(Named.as(sourceTopic + "-"))
                .branch((key, result) -> result.valid(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        branches.get(sourceTopic + "-invalid")
                .mapValues(result -> JsonSerdes.toJson(
                        new DlqMessage(sourceTopic, result.reason(), result.rawJson())))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        return branches.get(sourceTopic + "-valid").mapValues(ValidationResult::value);
    }
}
