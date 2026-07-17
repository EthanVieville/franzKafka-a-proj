package fr.esgi.kafka.cartflow.model;

import java.util.HashSet;
import java.util.Set;

/**
 * État interne de l'agrégation CART-4.
 *
 * Il conserve le nombre de commandes et les villes rencontrées
 * pour une carte dans une fenêtre de cinq minutes.
 */
public record FraudStats(
        long orderCount,
        Set<String> cities
) {

    public FraudStats {
        cities = cities == null
                ? Set.of()
                : Set.copyOf(cities);
    }

    public static FraudStats empty() {
        return new FraudStats(0, Set.of());
    }

    public FraudStats add(String city) {
        Set<String> updatedCities =
                new HashSet<>(cities);

        if (city != null && !city.isBlank()) {
            updatedCities.add(city);
        }

        return new FraudStats(
                orderCount + 1,
                updatedCities
        );
    }

    public boolean isFraud() {
        return orderCount >= 3
                && cities.size() >= 2;
    }
}