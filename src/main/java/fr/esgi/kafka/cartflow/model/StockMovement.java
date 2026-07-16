package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Mouvement de stock (topic cartflow.stock.movements, cle = sku).
 * delta negatif = sortie (vente), positif = reassort.
 */
public record StockMovement(
        @JsonProperty("sku") String sku,
        @JsonProperty("delta") Integer delta,
        @JsonProperty("warehouse") String warehouse,
        @JsonProperty("timestamp") String timestamp) {
}
