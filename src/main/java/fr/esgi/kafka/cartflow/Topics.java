package fr.esgi.kafka.cartflow;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String ORDERS = "cartflow.orders";
    public static final String PAYMENTS = "cartflow.payments";
    public static final String STOCK_MOVEMENTS = "cartflow.stock.movements";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE =
            System.getenv().getOrDefault("GROUPE", "grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("cartflow.dlq");
    public static final String ORDERS_CONFIRMED = out("cartflow.orders.confirmed");
    public static final String REVENUE_BY_CATEGORY = out("cartflow.revenue.by-category");
    public static final String ALERTS_FRAUD = out("cartflow.alerts.fraud");
    public static final String STOCK_LEVELS = out("cartflow.stock.levels");
    public static final String ALERTS_STOCK = out("cartflow.alerts.stock");
}
