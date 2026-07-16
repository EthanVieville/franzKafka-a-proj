package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Commande confirmee (CART-2) : commande + paiement AUTHORIZED rapproches sur order_id. */
public record OrderConfirmed(
        @JsonProperty("order") Order order,
        @JsonProperty("payment") Payment payment) {
}
