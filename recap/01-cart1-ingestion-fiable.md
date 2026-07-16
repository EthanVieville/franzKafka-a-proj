# CART-1 — Ingestion fiable

Un fichier par ticket dans `recap/`. Objectif : pouvoir réexpliquer, à l'oral,
sans relire le code, **ce qu'on a fait, pourquoi, et avec quelles notions**.

## 1. Ce que demande le ticket

Consommer les **trois** topics d'entrée (`cartflow.orders`, `cartflow.payments`,
`cartflow.stock.movements`), parser leur JSON, valider les champs métier
(requis non null, `total`/`amount` > 0, enums connues, timestamp ISO-8601
parsable), envoyer les messages invalides vers `<grp>.cartflow.dlq` **avec le
message original + la raison**, et surtout : **l'application ne doit jamais
crasher**, même sur un message cassé ("poison pill" : JSON tronqué, non-JSON,
vide).

Critères d'acceptation (README) : 10 min sans crash, DLQ motivée, aucun
message valide perdu.

## 2. Ce qu'on a construit

### Fichiers créés/modifiés

| Fichier | Rôle |
|---|---|
| `validation/ValidationResult.java` | Record générique `(value, valid, rawJson, reason)` — résultat d'une validation, avec factories `ok(...)` / `invalid(...)`. |
| `validation/Validators.java` | Une méthode statique de validation par type : `validateOrder`, `validatePayment`, `validateStockMovement`. Contient toutes les règles métier. |
| `model/DlqMessage.java` | Record `(source_topic, reason, original_message)` — la forme JSON qu'on écrit sur la DLQ. |
| `CartflowTopology.java` | Branche les 3 flux bruts sur `Validators`, route valide/invalide via une méthode générique `validateAndRouteToDlq(...)`. |
| `CartflowTopologyTest.java` | 9 tests `TopologyTestDriver` (6 invalides, 3 valides) + 1 `@Disabled` pour CART-2. |

### Le pipeline, en une phrase

`String brut` → **parse** (`JsonSerdes.parseOrNull`, ne lève jamais
d'exception) → **valide** (règles métier dans `Validators`) → **split()/branch()**
→ branche invalide vers `Topics.DLQ`, branche valide devient un
`KStream<String, Order|Payment|StockMovement>` typé, prêt pour CART-2..5.

## 3. Pourquoi ce design (à défendre à l'oral)

### Pourquoi séparer "parse" et "valide" en deux étapes ?

Un JSON peut être **lisible mais faux** (`{"total": -50}` — parse très bien,
mais `total` négatif est absurde métier) ou **illisible** (JSON tronqué —
poison pill). Ce sont deux familles d'erreurs différentes :
- Le **parsing** (`JsonSerdes.parseOrNull`) capture toute exception Jackson et
  renvoie `null` — jamais d'exception qui remonte au thread Kafka Streams.
- La **validation métier** (`Validators.validateXxx`) tourne ensuite sur
  l'objet Java déjà typé (record `Order`, `Payment`, `StockMovement`), donc du
  code simple, sans try/catch, avec des raisons de rejet explicites et
  lisibles dans la DLQ.

### Pourquoi ne pas désérialiser directement en `Order` dans `Consumed.with(...)` ?

C'est LE piège du sujet. Si on utilise un Serde Kafka Streams typé (via
`JsonSerdes.of(Order.class)`) directement à la lecture du topic, une erreur de
désérialisation déclenche le `deserialization.exception.handler`
(`LogAndFailExceptionHandler` par défaut, visible dans les logs de
`StreamsConfig`) → le thread Kafka Streams **s'arrête** sur un poison pill.
C'est exactement l'anomalie documentée dans le README : *"poison pill :
l'appli meurt en boucle"*.

**Solution** : on consomme toujours en `Serdes.String()` (`Consumed.with(Serdes.String(), Serdes.String())`), 
et on ne désérialise "à la main" qu'à l'intérieur d'un `mapValues`, où on
contrôle totalement les exceptions (`parseOrNull` capture tout). Aucune
exception ne peut donc jamais atteindre le framework Kafka Streams à ce stade.

### Pourquoi une méthode générique `validateAndRouteToDlq<T>(...)` plutôt que 3 blocs copiés-collés ?

Le routage valide/invalide (`split()`/`branch()` + écriture sur la DLQ) est
**identique** pour les 3 flux — seule la fonction de validation change. On
factorise ce squelette dans une méthode générique paramétrée par
`Function<String, ValidationResult<T>>`, appelée 3 fois avec une référence de
méthode différente (`Validators::validateOrder`, etc.). La logique métier,
elle, reste séparée par type dans `Validators` (pas de méga-méthode qui fait
tout).

### `split()`/`branch()` : pourquoi la nouvelle API ?

```java
Map<String, KStream<String, ValidationResult<T>>> branches = checked
        .split(Named.as(sourceTopic + "-"))
        .branch((key, result) -> result.valid(), Branched.as("valid"))
        .defaultBranch(Branched.as("invalid"));
```

L'ancienne API `stream.branch(Predicate...)` (qui renvoyait un tableau de
`KStream[]`) a été **supprimée en Kafka 4.0** (le README le signale). La
nouvelle API (`split()` → `.branch(predicate, Branched.as(name))` →
`.defaultBranch(...)`) renvoie une `Map<String, KStream<...>>` nommée
explicitement, plus lisible pour retrouver la branche "invalid" ensuite.

### Pourquoi un `Map` d'erreurs + raison plutôt que juste rejeter en silence ?

Le README l'exige ("DLQ motivée"), mais c'est aussi une pratique standard :
sans raison explicite, on ne peut jamais diagnostiquer *pourquoi* un flux de
production perd des messages. Le `DlqMessage` garde `source_topic` (utile
puisque les 3 flux partagent la même DLQ), `reason` (phrase courte,
déterministe, testable), et `original_message` (le JSON brut — permet de
rejouer le message une fois le bug corrigé).

## 4. Bug trouvé et corrigé pendant les tests

`Validators.validateStockMovement` faisait `WAREHOUSES.contains(movement.warehouse())` 
où `WAREHOUSES` est un `Set.of(...)`. **En Java, `Set.of(...).contains(null)`
lève une `NullPointerException`** (les collections immuables créées par
`Set.of`/`List.of`/`Map.of` interdisent `null` et lèvent NPE dès qu'on les
interroge avec `null`, pas seulement à l'insertion). Résultat : un mouvement
de stock sans champ `warehouse` (anomalie "champ requis absent" du README)
aurait fait planter le thread Kafka Streams — exactement ce que CART-1 doit
empêcher.

**Correctif** : `isBlank(movement.warehouse()) || !WAREHOUSES.contains(...)`
— le `isBlank` (qui gère `null`) est évalué en premier, court-circuite le
`||`, et le `.contains(null)` n'est jamais atteint.

*A retenir pour l'oral : c'est un bon exemple concret de "champ manquant qui
aurait pu crasher l'appli" — utile si on te demande un exemple réel de poison
pill géré.*

## 5. Notions à savoir expliquer

- **KStream vs KTable** : ici on ne manipule que des `KStream` (flux
  d'événements bruts) — les `KTable` arriveront avec CART-5 (stock agrégé).
- **`StreamsBuilder` / `Topology`** : `StreamsBuilder` est un DSL qui
  construit une `Topology` (graphe de traitement) ; Spring Boot
  (`@EnableKafkaStreams`) prend le bean `StreamsBuilder`, appelle
  `builder.build()` et démarre le `KafkaStreams` client automatiquement.
- **Consumed / Produced** : déclarent les Serdes clé/valeur à la lecture
  (`Consumed.with(...)`) et à l'écriture (`Produced.with(...)`) d'un topic.
- **`mapValues` vs `map`** : `mapValues` ne touche pas la clé → Kafka Streams
  sait qu'aucun repartitionnement n'est nécessaire (optimisation importante,
  à contraster avec le `selectKey` de CART-2 qui lui *force* un
  repartitionnement).
- **Records Java** (`Order`, `Payment`, `StockMovement`, `DlqMessage`,
  `ValidationResult<T>`) : classes immuables, `equals`/`hashCode`/`toString`
  générés, syntaxe concise pour des objets de données purs.
- **Generics** (`ValidationResult<T>`, `Function<String, ValidationResult<T>>`) :
  permettent à `validateAndRouteToDlq` d'être écrite une seule fois et
  réutilisée pour `Order`, `Payment` et `StockMovement` sans dupliquer le
  code de routage.
- **`application.id` = `cartflow-<GROUPE>`** : c'est l'identifiant du groupe
  de consommateurs Kafka Streams — deux instances avec le même
  `application.id` se partagent le travail (partitions) ; deux `GROUPE`
  différents ont des offsets et des topics de sortie totalement isolés (d'où
  le préfixage obligatoire).
- **`auto.offset.reset=earliest`** : au premier démarrage (pas d'offset
  committé), on relit le topic depuis le début plutôt que d'ignorer
  l'historique.
- **Cluster KRaft** : 3 brokers Kafka en mode KRaft (sans Zookeeper, chaque
  broker cumule les rôles `broker` + `controller`) — c'est la nouvelle
  architecture Kafka 4.x standard.
- **`TopologyTestDriver`** : exécute la `Topology` en mémoire (pas de vrai
  broker, pas de vrai `KafkaStreams` client) — on pousse des messages dans un
  `TestInputTopic` et on lit un `TestOutputTopic`, ce qui rend les tests
  rapides et déterministes. C'est pour ça qu'on construit la topologie via
  `new CartflowTopology().pipeline(new StreamsBuilder())` directement, sans
  démarrer Spring.

## 6. Bugs d'environnement rencontrés (pas du code, mais bon à savoir)

- **`$env:GROUPE=...` ne marche pas dans le shell utilisé par Claude Code**
  (Git Bash, pas PowerShell) → syntaxe bash : `GROUPE=grp01 mvn spring-boot:run`.
  Sans ça, `GROUPE` retombe sur sa valeur par défaut (`grp00`, cf.
  `Topics.GROUPE = System.getenv().getOrDefault("GROUPE", "grp00")`).
- **`MissingSourceTopicException` au premier lancement** : Kafka Streams a
  besoin que les topics sources **existent déjà** au moment du rebalance —
  contrairement à un producteur classique, il ne suffit pas que
  `auto.create.topics.enable=true` soit activé sur le broker : si aucun
  producteur n'a jamais écrit sur `cartflow.orders`/`payments`/`stock.movements`,
  le topic n'existe pas et le partition assignor échoue, l'appli s'arrête
  (`SHUTDOWN_CLIENT`). Correction : création manuelle des 3 topics en CLI
  (`kafka-topics.sh --create`, 3 partitions / RF 3) avant de relancer l'appli.

## 7. Comment on a testé

- **Tests unitaires** (`CartflowTopologyTest`, `TopologyTestDriver`, 9 cas +
  1 disabled) : s'exécutent automatiquement à `mvn clean package` (phase
  `test` liée avant `package` par défaut, rien à configurer). Couvrent : champ
  requis absent, JSON tronqué (poison pill), timestamp illisible, enum
  inconnue, valeur hors bornes, et 3 cas "message valide → ne part pas en
  DLQ".
- **Test manuel sur le vrai cluster** : appli lancée (`GROUPE=grp01`), messages
  produits à la main via Kafbat UI (`localhost:8080`) sur `cartflow.orders`,
  vérification du contenu de `grp01.cartflow.dlq`.

## 8. Ce qui n'est PAS fait (scope restant)

- Rien n'est encore branché sur une sortie "métier" — `validPayments` et
  `validStock` sont calculés mais pas encore consommés (normal, c'est CART-2
  et CART-5). Le `@Bean pipeline(...)` retourne `validOrders` juste pour que
  la signature compile.
- Pas de gestion des doublons ni des retardataires (ça concerne les fenêtres
  de CART-2/3/4, pas la validation CART-1).
- Le `LOG.info`/`peek` fourni initialement a été supprimé (demandé par le
  README).

## 9. Questions probables à l'oral (anticipation)

- *"Pourquoi ne pas juste utiliser le Serde JSON directement sur le topic
  source ?"* → réponse en section 3 (poison pill / `deserialization.exception.handler`).
- *"Que se passe-t-il si Jackson réussit à parser un JSON avec un mauvais
  type (`"total": "douze"`) ?"* → Jackson lève une exception de désérialisation
  → capturée par `parseOrNull` → `order == null` → raison `"JSON illisible ou
  tronque"` (on ne distingue pas "mauvais type" de "JSON cassé" dans le
  message actuel : point à assumer/discuter, amélioration possible si
  demandé en direct : distinguer les deux raisons).
- *"Pourquoi la DLQ est-elle un seul topic partagé entre les 3 flux et pas
  trois DLQ séparées ?"* → simplicité + le champ `source_topic` suffit à
  distinguer l'origine ; un choix à assumer, discutable.
- *"Comment tu garantis qu'aucun message valide n'est perdu ?"* → chaque
  branche `valid`/`invalid` du `split()` couvre 100% des messages entrants
  (pas de 3ème branche silencieuse) ; les tests "message valide" vérifient
  explicitement qu'ils ne finissent pas en DLQ.
