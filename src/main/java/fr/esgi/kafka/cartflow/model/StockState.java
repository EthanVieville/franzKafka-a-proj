package fr.esgi.kafka.cartflow.model;

/** État interne de CART-5 : niveau précédent et niveau courant. */
public record StockState(int previousLevel, int currentLevel) {

    public static StockState initial() {
        return new StockState(100, 100);
    }

    public StockState apply(int delta) {
        return new StockState(currentLevel, currentLevel + delta);
    }

    public boolean crossedBelow(int threshold) {
        return previousLevel >= threshold && currentLevel < threshold;
    }
}