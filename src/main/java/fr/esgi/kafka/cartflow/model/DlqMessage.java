package fr.esgi.kafka.cartflow.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Message envoye sur la DLQ : topic source, raison du rejet, JSON original. */
public record DlqMessage(
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("reason") String reason,
        @JsonProperty("original_message") String originalMessage) {
}
