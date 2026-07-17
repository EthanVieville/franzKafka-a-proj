package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Niveau courant d'un SKU. */
public record StockLevel(
        @JsonProperty("sku") String sku,
        @JsonProperty("level") int level
) {
}