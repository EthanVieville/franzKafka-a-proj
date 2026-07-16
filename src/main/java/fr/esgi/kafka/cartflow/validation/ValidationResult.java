package fr.esgi.kafka.cartflow.validation;

/**
 * Resultat de la validation d'un message brut : soit la valeur typee
 * (message valide), soit une raison de rejet accompagnee du JSON original
 * (message invalide, destine a la DLQ).
 */
public record ValidationResult<T>(T value, boolean valid, String rawJson, String reason) {

    public static <T> ValidationResult<T> ok(T value, String rawJson) {
        return new ValidationResult<>(value, true, rawJson, null);
    }

    public static <T> ValidationResult<T> invalid(String rawJson, String reason) {
        return new ValidationResult<>(null, false, rawJson, reason);
    }
}
