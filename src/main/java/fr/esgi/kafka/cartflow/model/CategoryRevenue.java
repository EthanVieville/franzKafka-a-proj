package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** CA d'une catégorie sur une fenêtre tumbling de 1 min (CART-3). */
public record CategoryRevenue(
        @JsonProperty("category") String category,
        @JsonProperty("window_start") long windowStart,
        @JsonProperty("window_end") long windowEnd,
        @JsonProperty("revenue") double revenue
) {
}
