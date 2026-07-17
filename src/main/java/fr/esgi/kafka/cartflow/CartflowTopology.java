package fr.esgi.kafka.cartflow;

import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.CategoryRevenue;
import fr.esgi.kafka.cartflow.model.DlqMessage;
import fr.esgi.kafka.cartflow.model.FraudAlert;
import fr.esgi.kafka.cartflow.model.FraudStats;
import fr.esgi.kafka.cartflow.model.Order;
import fr.esgi.kafka.cartflow.model.OrderConfirmed;
import fr.esgi.kafka.cartflow.model.Payment;
import fr.esgi.kafka.cartflow.model.StockAlert;
import fr.esgi.kafka.cartflow.model.StockLevel;
import fr.esgi.kafka.cartflow.model.StockMovement;
import fr.esgi.kafka.cartflow.model.StockState;
import fr.esgi.kafka.cartflow.validation.ValidationResult;
import fr.esgi.kafka.cartflow.validation.Validators;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
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

    private static final Logger LOG = LoggerFactory.getLogger(CartflowTopology.class);

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

        // -----------------------------------------------------------------
        // CART-2 - Commandes confirmees -> Topics.ORDERS_CONFIRMED
        //
        // Les deux flux arrivent initialement avec card_id comme clé.
        // La jointure doit être faite sur order_id : on reclé puis on
        // repartitionne explicitement.
        //
        // Seuls les paiements AUTHORIZED sont conservés.
        //
        // Fenêtre symétrique de 10 minutes :
        // une commande et un paiement sont associés lorsque leurs timestamps
        // Kafka diffèrent de 10 minutes maximum.
        //
        // Grâce de 1 minute :
        // un événement appartenant à la fenêtre peut arriver jusqu'à une
        // minute en retard par rapport au stream time.
        // -----------------------------------------------------------------

        KStream<String, Payment> authorizedPayments =
                validPayments.filter(
                        (cardId, payment) ->
                                "AUTHORIZED".equals(
                                        payment.status()
                                )
                );

        KStream<String, Order> ordersByOrderId =
                validOrders
                        .selectKey(
                                (cardId, order) ->
                                        order.orderId()
                        )
                        .repartition(
                                Repartitioned.with(
                                                Serdes.String(),
                                                JsonSerdes.of(Order.class)
                                        )
                                        .withName(
                                                "orders-by-order-id"
                                        )
                        );

        KStream<String, Payment> paymentsByOrderId =
                authorizedPayments
                        .selectKey(
                                (cardId, payment) ->
                                        payment.orderId()
                        )
                        .repartition(
                                Repartitioned.with(
                                                Serdes.String(),
                                                JsonSerdes.of(Payment.class)
                                        )
                                        .withName(
                                                "payments-by-order-id"
                                        )
                        );

        KStream<String, OrderConfirmed> confirmedOrders =
                ordersByOrderId.join(
                        paymentsByOrderId,

                        OrderConfirmed::new,

                        JoinWindows.ofTimeDifferenceAndGrace(
                                Duration.ofMinutes(10),
                                Duration.ofMinutes(5)
                        ),

                        StreamJoined
                                .<String, Order, Payment>as(
                                        "orders-payments-join"
                                )
                                .withKeySerde(
                                        Serdes.String()
                                )
                                .withValueSerde(
                                        JsonSerdes.of(Order.class)
                                )
                                .withOtherValueSerde(
                                        JsonSerdes.of(Payment.class)
                                )
                );

        confirmedOrders.to(
                Topics.ORDERS_CONFIRMED,
                Produced.with(
                        Serdes.String(),
                        JsonSerdes.of(OrderConfirmed.class)
                )
        );

        // -----------------------------------------------------------------
        // CART-3 - CA par categorie -> Topics.REVENUE_BY_CATEGORY
        //   Source : commandes brutes valides (CART-1), pas les commandes
        //   confirmees de CART-2 - choix assume : le CA est disponible
        //   immediatement (pas d'attente du join sur le paiement), au prix
        //   de compter aussi les ~12% de commandes jamais payees (a
        //   defendre a l'oral comme un choix, pas un oubli).
        //   flatMap "eclate" chaque commande en une ligne par item (une
        //   commande peut contenir plusieurs categories) ; le flatMap
        //   change la cle (card_id -> category), donc Kafka Streams insere
        //   automatiquement un repartitionnement avant l'aggregation
        //   (meme mecanisme implicite que pour confirmedByCard en CART-4,
        //   pas besoin de repartition() explicite ici).
        //   Fenetre tumbling 1 min (blocs fixes, non chevauchants -
        //   different du SlidingWindows de CART-4) ; emission en continu
        //   (pas de suppress()), coherent avec la publication continue du
        //   stock en CART-5 : chaque commande met a jour le total de sa
        //   fenetre et le republie aussitot.
        // -----------------------------------------------------------------
        KStream<String, Double> categoryRevenueLines = validOrders
                .flatMap((cardId, order) -> order.items().stream()
                        .map(item -> KeyValue.pair(
                                item.category(),
                                item.qty() * item.unitPrice()))
                        .toList());

        KTable<Windowed<String>, Double> revenueByCategory = categoryRevenueLines
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .reduce(Double::sum,
                        Named.as("revenue-by-category"),
                        Materialized.with(Serdes.String(), Serdes.Double()));

        revenueByCategory.toStream()
                .map((windowedCategory, revenue) -> KeyValue.pair(
                        windowedCategory.key(),
                        new CategoryRevenue(
                                windowedCategory.key(),
                                windowedCategory.window().start(),
                                windowedCategory.window().end(),
                                revenue)))
                .to(Topics.REVENUE_BY_CATEGORY,
                        Produced.with(Serdes.String(), JsonSerdes.of(CategoryRevenue.class)));

        // CART-4 - Detection de fraude carte           -> Topics.ALERTS_FRAUD
        //
        // Alerte lorsqu'une carte effectue au moins trois commandes dans
        // une période glissante de cinq minutes, avec au moins deux villes
        // de livraison différentes.
        //
        // Source : commandes confirmées de CART-2.
        // Clé de regroupement : card_id.
        // -----------------------------------------------------------------

        KStream<String, OrderConfirmed> confirmedByCard =
                confirmedOrders.selectKey(
                        (orderId, confirmed) ->
                                confirmed.order().cardId()
                );

        KTable<Windowed<String>, FraudStats> fraudWindows =
                confirmedByCard
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        JsonSerdes.of(OrderConfirmed.class)
                                )
                        )
                        .windowedBy(
                                SlidingWindows.ofTimeDifferenceAndGrace(
                                        Duration.ofMinutes(5),
                                        Duration.ofMinutes(5)
                                )
                        )
                        .aggregate(
                                FraudStats::empty,

                                (cardId, confirmed, stats) ->
                                        stats.add(
                                                confirmed.order()
                                                        .shippingCity()
                                        ),

                                Named.as("fraud-by-card"),

                                Materialized.with(
                                        Serdes.String(),
                                        JsonSerdes.of(FraudStats.class)
                                )
                        );

        fraudWindows
                .toStream()
                .filter(
                        (windowedCard, stats) ->
                                stats != null
                                        && stats.isFraud()
                )
                .map(
                        (windowedCard, stats) ->
                                KeyValue.pair(
                                        windowedCard.key(),
                                        new FraudAlert(
                                                windowedCard.key(),
                                                stats.orderCount(),
                                                stats.cities(),
                                                windowedCard.window().start(),
                                                windowedCard.window().end()
                                        )
                                )
                )
                .to(
                        Topics.ALERTS_FRAUD,
                        Produced.with(
                                Serdes.String(),
                                JsonSerdes.of(FraudAlert.class)
                        )
                );
        // -----------------------------------------------------------------
        // CART-5 - Stock courant et alerte de rupture
        //   Stock initial : 100 pour chaque SKU rencontré.
        //   Chaque delta est ajouté au niveau courant.
        //   Une alerte est produite uniquement lors du passage de >= 10 à < 10.
        // -----------------------------------------------------------------
        final int stockAlertThreshold = 10;

        KTable<String, StockState> stockStates = validStock
                .selectKey((inputKey, movement) -> movement.sku())
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        JsonSerdes.of(StockMovement.class)))
                .aggregate(
                        StockState::initial,
                        (sku, movement, state) -> state.apply(movement.delta()),
                        Named.as("stock-by-sku"),
                        Materialized.with(
                                Serdes.String(),
                                JsonSerdes.of(StockState.class)));

        // Publication continue du niveau après chaque mouvement.
        stockStates.toStream()
                .mapValues((sku, state) -> new StockLevel(sku, state.currentLevel()))
                .to(Topics.STOCK_LEVELS,
                        Produced.with(Serdes.String(), JsonSerdes.of(StockLevel.class)));

        // Publication uniquement au moment du franchissement du seuil.
        stockStates.toStream()
                .filter((sku, state) -> state.crossedBelow(stockAlertThreshold))
                .mapValues((sku, state) -> new StockAlert(
                        sku,
                        state.currentLevel(),
                        stockAlertThreshold))
                .to(Topics.ALERTS_STOCK,
                        Produced.with(Serdes.String(), JsonSerdes.of(StockAlert.class)));
        // CART-6 (bonus) - exactly_once_v2 + demonstration (cf. README)

        LOG.info("Topologie construite :\n{}", builder.build().describe());

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
