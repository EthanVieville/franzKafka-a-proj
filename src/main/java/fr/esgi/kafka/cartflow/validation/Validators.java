package fr.esgi.kafka.cartflow.validation;

import fr.esgi.kafka.cartflow.common.JsonSerdes;
import fr.esgi.kafka.cartflow.model.Order;
import fr.esgi.kafka.cartflow.model.Payment;
import fr.esgi.kafka.cartflow.model.StockMovement;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/** Regles de validation metier par flux (README - tableau des anomalies). */
public final class Validators {

    private static final Set<String> CATEGORIES = Set.of(
            "HIGH-TECH", "MAISON", "MODE", "SPORT", "BEAUTE", "JARDIN", "JOUETS", "EPICERIE");
    private static final Set<String> PAYMENT_STATUSES = Set.of("AUTHORIZED", "DECLINED");
    private static final Set<String> WAREHOUSES = Set.of("ORLY-1", "LILLE-2", "LYON-3");

    private Validators() {
    }

    public static ValidationResult<Order> validateOrder(String json) {
        Order order = JsonSerdes.parseOrNull(json, Order.class);
        if (order == null) {
            return ValidationResult.invalid(json, "JSON illisible ou tronque");
        }
        if (isBlank(order.orderId())) {
            return ValidationResult.invalid(json, "order_id manquant");
        }
        if (isBlank(order.userId())) {
            return ValidationResult.invalid(json, "user_id manquant");
        }
        if (isBlank(order.cardId())) {
            return ValidationResult.invalid(json, "card_id manquant");
        }
        if (isBlank(order.currency())) {
            return ValidationResult.invalid(json, "currency manquant");
        }
        if (isBlank(order.shippingCity())) {
            return ValidationResult.invalid(json, "shipping_city manquant");
        }
        if (order.total() == null || order.total() <= 0) {
            return ValidationResult.invalid(json, "total manquant ou <= 0");
        }
        if (!isParsableTimestamp(order.timestamp())) {
            return ValidationResult.invalid(json, "timestamp illisible");
        }
        if (order.items() == null || order.items().isEmpty()) {
            return ValidationResult.invalid(json, "items manquants ou vide");
        }
        for (Order.Item item : order.items()) {
            if (isBlank(item.sku()) || isBlank(item.product()) || isBlank(item.category())) {
                return ValidationResult.invalid(json, "item incomplet (sku/product/category)");
            }
            if (!CATEGORIES.contains(item.category())) {
                return ValidationResult.invalid(json, "categorie inconnue: " + item.category());
            }
            if (item.qty() == null || item.qty() <= 0) {
                return ValidationResult.invalid(json, "item qty invalide");
            }
            if (item.unitPrice() == null || item.unitPrice() <= 0) {
                return ValidationResult.invalid(json, "item unit_price invalide");
            }
        }
        return ValidationResult.ok(order, json);
    }

    public static ValidationResult<Payment> validatePayment(String json) {
        Payment payment = JsonSerdes.parseOrNull(json, Payment.class);
        if (payment == null) {
            return ValidationResult.invalid(json, "JSON illisible ou tronque");
        }
        if (isBlank(payment.paymentId())) {
            return ValidationResult.invalid(json, "payment_id manquant");
        }
        if (isBlank(payment.orderId())) {
            return ValidationResult.invalid(json, "order_id manquant");
        }
        if (isBlank(payment.cardId())) {
            return ValidationResult.invalid(json, "card_id manquant");
        }
        if (payment.amount() == null || payment.amount() <= 0) {
            return ValidationResult.invalid(json, "amount manquant ou <= 0");
        }
        if (payment.status() == null || !PAYMENT_STATUSES.contains(payment.status())) {
            return ValidationResult.invalid(json, "status inconnu: " + payment.status());
        }
        if (!isParsableTimestamp(payment.timestamp())) {
            return ValidationResult.invalid(json, "timestamp illisible");
        }
        return ValidationResult.ok(payment, json);
    }

    public static ValidationResult<StockMovement> validateStockMovement(String json) {
        StockMovement movement = JsonSerdes.parseOrNull(json, StockMovement.class);
        if (movement == null) {
            return ValidationResult.invalid(json, "JSON illisible ou tronque");
        }
        if (isBlank(movement.sku())) {
            return ValidationResult.invalid(json, "sku manquant");
        }
        if (movement.delta() == null) {
            return ValidationResult.invalid(json, "delta manquant");
        }
        if (isBlank(movement.warehouse()) || !WAREHOUSES.contains(movement.warehouse())) {
            return ValidationResult.invalid(json, "warehouse inconnu: " + movement.warehouse());
        }
        if (!isParsableTimestamp(movement.timestamp())) {
            return ValidationResult.invalid(json, "timestamp illisible");
        }
        return ValidationResult.ok(movement, json);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static boolean isParsableTimestamp(String value) {
        if (isBlank(value)) {
            return false;
        }
        try {
            Instant.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
