package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Paiement (topic cartflow.payments, cle = card_id).
 * Arrive entre 5 s et 8 min apres la commande... quand il arrive
 * (~12 % des commandes ne sont jamais payees).
 * status : AUTHORIZED | DECLINED.
 */
public record Payment(
        @JsonProperty("payment_id") String paymentId,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("amount") Double amount,
        @JsonProperty("status") String status,
        @JsonProperty("timestamp") String timestamp) {
}
