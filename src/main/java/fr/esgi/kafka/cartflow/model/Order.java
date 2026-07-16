package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Commande (topic cartflow.orders, cle = card_id). */
public record Order(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("card_id") String cardId,
        @JsonProperty("items") List<Item> items,
        @JsonProperty("total") Double total,
        @JsonProperty("currency") String currency,
        @JsonProperty("shipping_city") String shippingCity,
        @JsonProperty("timestamp") String timestamp) {

    /** Ligne de commande. */
    public record Item(
            @JsonProperty("sku") String sku,
            @JsonProperty("product") String product,
            @JsonProperty("category") String category,
            @JsonProperty("qty") Integer qty,
            @JsonProperty("unit_price") Double unitPrice) {
    }
}
