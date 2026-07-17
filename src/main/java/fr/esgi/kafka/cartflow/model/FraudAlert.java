package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Alerte produite lorsqu'une carte passe au moins trois commandes
 * dans une fenêtre glissante de cinq minutes et utilise au moins
 * deux villes différentes.
 */
public record FraudAlert(
        @JsonProperty("card_id")
        String cardId,

        @JsonProperty("order_count")
        long orderCount,

        @JsonProperty("cities")
        Set<String> cities,

        @JsonProperty("window_start")
        long windowStart,

        @JsonProperty("window_end")
        long windowEnd
) {
    public FraudAlert {
        cities = cities == null
                ? Set.of()
                : Set.copyOf(cities);
    }
}