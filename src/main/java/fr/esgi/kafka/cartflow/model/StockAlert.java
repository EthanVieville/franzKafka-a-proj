package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Alerte émise lors du passage d'un SKU sous le seuil de rupture. */
public record StockAlert(
        @JsonProperty("sku") String sku,
        @JsonProperty("level") int level,
        @JsonProperty("threshold") int threshold
) {
}