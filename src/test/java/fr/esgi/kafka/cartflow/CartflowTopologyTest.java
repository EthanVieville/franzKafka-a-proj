package fr.esgi.kafka.cartflow;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : TopologyTestDriver (deja en scope test). On teste la Topology
 * nue construite via StreamsBuilder, sans demarrer Spring ni broker.
 */
class CartflowTopologyTest {

    @Test
    @Disabled("A implementer : un message invalide doit finir dans la DLQ")
    void messageInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : commande + paiement AUTHORIZED sous 10 min = confirmee")
    void commandePayeeEstConfirmee() {
    }
}
