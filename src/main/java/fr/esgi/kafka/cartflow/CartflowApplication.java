package fr.esgi.kafka.cartflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Point d'entree (fourni). La topologie se construit dans
 * {@link CartflowTopology}. Lancement :
 *
 *   GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn spring-boot:run
 */
@SpringBootApplication
public class CartflowApplication {

    public static void main(String[] args) {
        SpringApplication.run(CartflowApplication.class, args);
    }
}
