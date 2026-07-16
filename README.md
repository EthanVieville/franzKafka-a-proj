# Projet CARTFLOW — Pipeline e-commerce temps réel

*Framework imposé : **Spring Boot 4.1 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Order Pipeline de **CARTFLOW**, une marketplace
e-commerce (pensez Amazon). Trois flux arrivent en continu : les commandes,
les paiements (qui arrivent **entre 5 secondes et 8 minutes après** la
commande… quand ils arrivent : ~12 % des paniers ne sont jamais payés), et les
mouvements de stock. Le métier veut : des **commandes confirmées** (commande +
paiement autorisé rapprochés), le **chiffre d'affaires par catégorie** à la
minute, une **alerte fraude** quand une carte enchaîne les commandes depuis
plusieurs villes, et le **stock courant** avec alerte de rupture.

Les intégrations partenaires envoient des messages cassés : champs manquants,
types invalides, JSON tronqué, doublons, retardataires. **Le flux ne sera
jamais propre : c'est à votre pipeline d'être robuste.**

## Mission

Construire une application **Spring Boot + Kafka Streams** qui consomme les
trois flux, écarte proprement les messages invalides, rapproche commandes et
paiements, et produit CA, alertes fraude et niveaux de stock.

## Architecture

```
cartflow.orders ──────────┐
cartflow.payments ────────┼─> [ VALIDATION ] ──> invalides ──> <grp>.cartflow.dlq
cartflow.stock.movements ─┘        │
                                   ▼ valides
        ┌──────────────────┬───────┴──────────┬─────────────────────┐
        ▼                  ▼                  ▼                     ▼
  join orders×payments   CA/catégorie    fraude carte         stock courant
  (fenêtre 10 min)       (tumbling 1min) (≥3 cmd/5min,        (somme des deltas)
        │                  │              villes ≠)             │         │
        ▼                  ▼                  ▼                 ▼         ▼
 <grp>.cartflow.    <grp>.cartflow.   <grp>.cartflow.   <grp>.cartflow. <grp>.cartflow.
 orders.confirmed   revenue.          alerts.fraud      stock.levels   alerts.stock
                    by-category
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`cartflow.orders`** — clé : `card_id` :

```json
{
  "order_id": "ord-2095eef6",
  "user_id": "u-0319",
  "card_id": "card-0160",
  "items": [
    {
      "sku": "sku-0003",
      "product": "Drone Zen 38",
      "category": "HIGH-TECH",
      "qty": 1,
      "unit_price": 1321.10
    }
  ],
  "total": 1321.10,
  "currency": "EUR",
  "shipping_city": "Lyon",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

Catégories : `HIGH-TECH, MAISON, MODE, SPORT, BEAUTE, JARDIN, JOUETS, EPICERIE`.

**`cartflow.payments`** — clé : `card_id` — `status` ∈ `AUTHORIZED | DECLINED`
(92 % AUTHORIZED). Le lien avec la commande est `order_id` :

```json
{
  "payment_id": "pay-91c22d01",
  "order_id": "ord-2095eef6",
  "card_id": "card-0160",
  "amount": 1321.10,
  "status": "AUTHORIZED",
  "timestamp": "2026-07-04T14:05:47.020Z"
}
```

**`cartflow.stock.movements`** — clé : `sku` — `delta` négatif = vente,
positif = réassort (un réassort aléatoire passe toutes les ~8 s) :

```json
{
  "sku": "sku-0003",
  "delta": -1,
  "warehouse": "ORLY-1",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

Entrepôts : `ORLY-1, LILLE-2, LYON-3`.

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                   | Impact si non gérée                       |
|----------------------------------------|---------------------------|-------------------------------------------|
| Champ requis absent                    | pas d'`order_id`          | paiement irrapprochable                   |
| Champ à `null`                         | `"total": null`           | NullPointerException                      |
| Mauvais type                           | `"total": "douze"`        | crash de désérialisation                  |
| Valeur hors bornes                     | `total: -50`              | CA faussé                                 |
| Enum inconnue                          | `"status": "AUTHORIZEDD"` | commande jamais confirmée                 |
| Timestamp illisible                    | `"hier a 15h"`            | jointure fenêtrée cassée                  |
| JSON tronqué / non-JSON / message vide | `{"order_id": "or`        | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min  | fenêtres faussées                         |
| Doublon exact                          | même `order_id` deux fois | CA double-compté                          |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.cartflow.dlq` · `<grp>.cartflow.orders.confirmed` ·
`<grp>.cartflow.revenue.by-category` · `<grp>.cartflow.alerts.fraud` ·
`<grp>.cartflow.stock.levels` · `<grp>.cartflow.alerts.stock` — constantes
prêtes dans `Topics.java`. Format des valeurs libre **mais en JSON documenté
dans votre README de rendu**.

## Backlog

> La base 8/20 = CART-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**CART-1 — Ingestion fiable (socle, obligatoire)**
Consommer les **trois** topics, parser, valider (champs requis, types,
`total`/`amount` > 0, enums, timestamp ISO-8601 parsable). Invalides → DLQ
**avec message original + raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**CART-2 — Commandes confirmées** → `<grp>.cartflow.orders.confirmed`
**Jointure stream-stream** commandes × paiements sur `order_id`, fenêtre de
**10 minutes**, en ne gardant que les paiements `AUTHORIZED`. Attention : les
deux flux sont clés par `card_id`, pas par `order_id` (`selectKey` obligatoire
→ repartitionnement, à expliquer à l'oral).
*Critères : une commande payée sous 10 min ressort confirmée ; une commande
jamais payée ne ressort pas ; le repartitionnement est visible dans
`topology.describe()` et expliqué.*

**CART-3 — CA par catégorie** → `<grp>.cartflow.revenue.by-category`
Somme de `qty × unit_price` par `category` sur **fenêtres tumbling de 1 min**
(il faut « éclater » les lignes de commande : `flatMap`). Basez-vous sur les
commandes **confirmées** de CART-2 si fait, sinon sur les commandes brutes
(précisez votre choix).
*Critères : totaux cohérents avec un calcul manuel sur 1 minute ; clé = catégorie.*

**CART-4 — Fraude carte** → `<grp>.cartflow.alerts.fraud`
Alerte quand une `card_id` passe **≥ 3 commandes en 5 minutes** avec **au
moins 2 `shipping_city` différentes**. Le générateur simule une carte volée
toutes les ~5 min (4-5 commandes en ~90 s, villes différentes).
*Critères : chaque incident `fraud_card` est détecté, l'alerte liste les
villes ; pas de faux positifs sur un client fidèle mono-ville.*

**CART-5 — Stock courant + alerte rupture** → `<grp>.cartflow.stock.levels`
et `<grp>.cartflow.alerts.stock`
Somme cumulée des `delta` par `sku` (KTable, `aggregate`), en partant d'un
stock initial de **100 par SKU**. Alerte quand le niveau passe **sous 10**. Le
générateur provoque une razzia (`stock_run`) toutes les ~7 min.
*Critères : niveaux publiés en continu ; chaque `stock_run` déclenche l'alerte ;
le niveau remonte avec les réassorts.*

**CART-6 (bonus) — Exactly-once**
Activer `processing.guarantee=exactly_once_v2` (préparé en commentaire dans
`application.yml`) et **démontrer** la différence : expliquer à l'oral ce qui
change (transactions, isolation.level des consommateurs aval) et ce que ça
coûte.
*Critères : configuration active + explication correcte du mécanisme ; savoir
dire ce qu'EOS ne protège PAS (le producteur d'origine).*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Lancer l'application
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn spring-boot:run
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn spring-boot:run
```

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

Sans générateur en local, créez les topics dans Kafbat UI et produisez les
exemples JSON ci-dessus à la main (pour CART-2, produisez la commande puis le
paiement avec le même `order_id`). Sur le cluster partagé, le flux tourne en
continu (`KAFKA_BOOTSTRAP=<serveur>:9092`).

## Contraintes

- Java 21, Spring Boot + Kafka Streams uniquement (pas de Spark/Flink, pas de
  `@KafkaListener` pour la logique métier), pas de base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`cartflow-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                    | Points |
|------------------------------------------------------------|--------|
| Socle CART-1 en production 10 min sans crash + DLQ motivée | **8**  |
| CART-2 (join fenêtré + selectKey)                          | +3     |
| CART-3 (CA par catégorie)                                  | +2     |
| CART-4 (fraude multi-villes)                               | +3     |
| CART-5 (stock + alerte)                                    | +2     |
| CART-6 (EOS démontré) ou tests TopologyTestDriver sérieux  | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `LOG.info` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- CART-2 est LE ticket structurant : dessinez les clés à chaque étape avant de
  coder. `JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10))`
  pour commencer, puis discutez du grace à l'oral (les retardataires !).
- CART-4 : agrégat fenêtré qui accumule `{count, villes}` dans un seul objet,
  puis filtre — plus simple que deux pipelines.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
