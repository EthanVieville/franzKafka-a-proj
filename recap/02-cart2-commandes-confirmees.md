# CART-2 — Commandes confirmées

Suite de `01-cart1-ingestion-fiable.md`. Même objectif : pouvoir réexpliquer
à l'oral, sans relire le code, **ce qu'on a fait, pourquoi, et avec quelles
notions** — CART-2 est annoncé dans le README comme *"LE ticket
structurant"*, c'est celui qui sera le plus creusé en soutenance.

## 1. Ce que demande le ticket

Produire des **commandes confirmées** (`<grp>.cartflow.orders.confirmed`) en
rapprochant, sur `order_id`, les commandes validées (CART-1) et les
paiements validés dont le `status` est `AUTHORIZED`, dans une **fenêtre de
10 minutes**. Piège annoncé par le README : les deux flux sont clés par
`card_id`, pas par `order_id` — un `selectKey` (donc un repartitionnement)
est obligatoire pour pouvoir joindre sur la bonne clé.

Critères d'acceptation :
- une commande payée (AUTHORIZED) sous 10 min ressort confirmée ;
- une commande jamais payée **ne ressort jamais** ;
- le repartitionnement est visible dans `topology.describe()` **et
  expliqué**.

## 2. Ce qu'on a construit

### Fichiers créés/modifiés

| Fichier | Rôle |
|---|---|
| `model/OrderConfirmed.java` | Record `(order: Order, payment: Payment)` — la sortie du join, format libre, on garde les deux objets complets imbriqués. |
| `CartflowTopology.java` | Filtre `AUTHORIZED`, `selectKey`+`repartition` nommés sur les deux flux, join `INNER` fenêtré, log de `topology.describe()` au démarrage. |
| `application.yml` | `max.task.idle.ms: 10000` (voir section 4 — c'est LE bug de ce ticket). |
| `tools/DataLoader.java` | Crée aussi `Topics.ORDERS_CONFIRMED` explicitement (3 partitions), pas seulement les topics source + DLQ. |
| `CartflowTopologyTest.java` | 4 nouveaux tests `TopologyTestDriver` (payée à temps, jamais payée, hors fenêtre, `DECLINED`). |

### Le flux des clés (à dessiner au tableau si demandé)

```
validOrders   (cle=card_id) --selectKey(order_id)--------------> repartition --\
                                                                                 join INNER, fenetre 10 min --> <grp>.cartflow.orders.confirmed (cle=order_id)
validPayments (cle=card_id) --filter(AUTHORIZED)--selectKey(order_id)-> repartition --/
```

## 3. Pourquoi ce design (à défendre à l'oral)

### Pourquoi filtrer `AUTHORIZED` *avant* le `selectKey`/repartition, pas après ?

Le repartitionnement a un coût (un vrai aller-retour producteur → topic
interne → consommateur). Filtrer les paiements `DECLINED` avant évite de
repartitionner des messages qu'on va de toute façon exclure du join. Sur les
vraies données : ~8% des paiements valides sont `DECLINED`, c'est plusieurs
milliers de messages évités.

### Pourquoi un `join` (INNER) et pas un `leftJoin` ?

C'est le point le plus important du ticket. Un `leftJoin` émettrait quand
même la commande, avec `payment = null`, une fois la fenêtre fermée (ou même
avant, selon le timing). Le critère *"une commande jamais payée ne ressort
pas"* exige explicitement l'inverse : silence total. Seul un `join` INNER
garantit qu'**aucune ligne n'est produite sans les deux côtés présents**.

### `selectKey` + `.repartition(...)` explicite : pourquoi, et pourquoi nommé ?

Les deux flux sont clés par `card_id` à la source. Pour joindre sur
`order_id`, il faut changer la clé — et Kafka Streams exige que les deux
côtés d'un join soient **co-partitionnés** (même clé, même nombre de
partitions, même partitioner) pour garantir qu'un `order_id` donné atterrit
sur la même partition des deux côtés. Changer la clé casse cette garantie,
d'où le repartitionnement obligatoire (Kafka Streams l'aurait de toute façon
inséré implicitement si on ne l'avait pas fait nous-même — on l'a rendu
**explicite et nommé** (`Repartitioned.with(...).withName("orders-by-order-id")`)
pour qu'il apparaisse clairement et lisiblement dans `topology.describe()`,
au lieu d'un nom généré automatiquement type `KSTREAM-FILTER-0000000024`.

### Fenêtre : `JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10))`

Fenêtre **symétrique** : le join accepte un écart de 10 min dans les
**deux sens** (paiement jusqu'à 10 min après *ou avant* la commande). Dans
notre cas métier, un paiement n'arrive jamais avant sa commande, donc en
pratique seul le sens "après" est utilisé — mais c'est un point à assumer à
l'oral (`.before(Duration.ZERO).after(Duration.ofMinutes(10))` donnerait une
fenêtre strictement asymétrique, plus proche de la sémantique métier réelle).
`NoGrace` = pas de délai de grâce supplémentaire après la fermeture nominale
de la fenêtre (voir section 4, où ça a une conséquence inattendue).

### Stores du join nommés (`StreamJoined.as("orders-payments-join")`)

Par défaut, Kafka Streams nomme les state stores d'un join
`KSTREAM-JOINTHIS-00000035-store` (numéro auto-incrémenté, illisible et pas
stable). On les a nommés explicitement pour deux raisons : lisibilité dans
`topology.describe()`, et stabilité — un nom auto-généré peut changer si on
modifie la topologie ailleurs, cassant la compatibilité avec un state déjà
sauvegardé en prod.

## 4. Bug trouvé et corrigé : `max.task.idle.ms`

**Symptôme** : en rejouant les ~250k messages réels, seulement **8 760
commandes confirmées** alors qu'on en attendait ~40 000 (calcul : ~52 000
commandes valides × ~88% payées × ~92% `AUTHORIZED`). Vérification sur une
paire commande/paiement connue, 20 secondes d'écart, largement dans la
fenêtre de 10 min : **absente** du topic confirmé, et absente aussi de la
DLQ (donc ni rejetée, ni confirmée — juste perdue).

**Cause** : `max.task.idle.ms` vaut `0` par défaut. Ça veut dire que Kafka
Streams **n'attend pas** d'avoir des données disponibles des deux côtés d'un
join avant de faire avancer son horloge interne ("stream time"). Avec
`DataLoader` qui envoie tout un fichier orders d'un coup, un run de milliers
de commandes peut être traité avant que le moindre paiement ne soit lu (le
consumer vide son buffer d'un côté et n'attend pas l'autre) — l'horloge
avance donc très loin sur la base des seules commandes, et referme des
fenêtres de 10 min *avant même que le paiement correspondant, pourtant à
l'heure, soit traité*. Avec `NoGrace`, une fenêtre fermée ne rouvre jamais.

**Correctif** : `max.task.idle.ms: 10000` dans `application.yml` — force
Kafka Streams à attendre (jusqu'à 10s) que les deux partitions aient des
données prêtes avant d'avancer. Testé à plusieurs valeurs pour confirmer le
diagnostic :

| `max.task.idle.ms` | Commandes confirmées | % du max théorique (~41k) |
|---|---|---|
| 0 (défaut) | 8 760 | 21% |
| 5 000 | 18 275 | 44% |
| 60 000 | 40 276 | **98%** |

*A retenir pour l'oral, c'est probablement LA question qu'on te posera sur
ce ticket : pourquoi le taux de confirmation est plus bas que prévu au
premier essai, et pourquoi ça n'arrive normalement pas en flux réel continu
(le générateur du prof produit orders/payments déjà entrelacés dans le
temps — ce problème est spécifique au rejeu en masse d'un dataset
pré-enregistré, pas à la logique du join elle-même, déjà validée
indépendamment par les tests unitaires `TopologyTestDriver`).* Les ~2%
restants même à `idle=60000` sont un résidu lié à `buffered.records.per.partition`
(1000 par défaut) — pas creusé plus loin, diminishing returns pour un
artefact de test qui ne se produit pas en conditions réelles.

## 5. Notions à savoir expliquer

- **Co-partitionnement** : deux flux ne peuvent être joints directement que
  s'ils ont la même clé, le même nombre de partitions et le même
  partitioner. C'est pour ça qu'un `selectKey` casse toujours cette
  garantie et force un repartitionnement avant un join ou un `groupBy`.
- **`JoinWindows` symétrique vs asymétrique** : `ofTimeDifferenceWithNoGrace(d)`
  accepte un écart `|t1 - t2| <= d` dans les deux sens ; `.before()`/`.after()`
  permettent une fenêtre asymétrique.
- **Grace period** : délai supplémentaire après la fermeture nominale d'une
  fenêtre pendant lequel des événements en retard sont encore acceptés.
  `NoGrace` = aucun, la fenêtre est définitivement fermée dès que le stream
  time la dépasse.
- **Stream time / `max.task.idle.ms`** : Kafka Streams calcule un temps
  logique par tâche à partir des timestamps des records qu'il a déjà traités
  (pas l'horloge murale). Avec plusieurs partitions en entrée d'une même
  tâche (cas d'un join), `max.task.idle.ms` contrôle si Kafka Streams
  attend d'avoir des données sur *toutes* les partitions avant d'avancer ce
  temps, ou s'il avance dès qu'une seule partition a des données —
  déterminant pour la correction d'un join en présence de partitions
  déséquilibrées.
- **Changelog topics** : chaque state store (ici, les deux côtés du join
  windowed) est sauvegardé dans un topic de changelog, pour pouvoir
  reconstruire l'état sans tout retraiter si l'appli redémarre ou change de
  machine. Visible dans Kafbat UI : `..._join-store-changelog`.
- **Sous-topologies** (`topology.describe()`) : chaque repartitionnement
  coupe la topologie en plusieurs sous-topologies indépendantes, connectées
  entre elles uniquement via des topics internes (jamais directement en
  mémoire). C'est pour ça qu'on voit 4 sous-topologies dans notre topologie
  (une par flux source + une pour le join).
- **Purge automatique des topics de repartitionnement** : une fois consommés,
  Kafka Streams supprime les données déjà traitées d'un topic de
  repartitionnement (`repartition.purge.interval.ms`, 30s par défaut) — ils
  ne sont pas faits pour être relus après coup, contrairement aux topics
  métier.

## 6. Bugs annexes rencontrés (pas la logique CART-2, mais instructifs)

- **`orders.confirmed` auto-créé avec 1 seule partition** au lieu de 3 : un
  topic jamais créé explicitement est auto-créé par le premier `producer.send()`
  avec le nombre de partitions par défaut du **broker** (1 ici), pas celui du
  reste du projet. Corrigé en le créant explicitement dans `DataLoader`,
  comme la DLQ.
- **`mvn compile` (sans `clean`) peut masquer une vraie erreur de
  compilation** en réutilisant un `.class` déjà compilé si le compilateur
  Maven juge (à tort, sur les timestamps) que rien n'a changé. On l'a
  découvert après un merge Git bâclé côté `DataLoader.java` (référence à une
  variable supprimée) : `mvn compile` répondait silencieusement "succès",
  `mvn clean compile` révélait 4 erreurs de compilation réelles. Réflexe à
  garder : toujours `clean` après un `pull`/merge avant de conclure que
  "ça compile".

## 7. Comment on a testé

- **Tests unitaires** (`TopologyTestDriver`, 4 nouveaux cas, pipeInput avec
  `Instant` explicite pour contrôler précisément l'écart temporel) :
  - `commandePayeeSousDixMinEstConfirmee` — paiement à +2 min → confirmée.
  - `commandeJamaisPayeeNeRessortJamais` — aucun paiement → rien ne sort.
  - `paiementHorsFenetreDeDixMinNeConfirmePas` — paiement à +11 min → rien.
  - `paiementDeclineNeConfirmePasLaCommande` — `DECLINED` → filtré avant le join.
- **Validation sur données réelles** (248k messages, `DataLoader`) : voir le
  tableau de la section 4. Résultat final : **40 276 commandes confirmées**,
  soit 98% du maximum théorique calculable à partir des taux d'anomalies
  connus (~52k commandes valides, ~41k paiements `AUTHORIZED` valides).

## 8. Ce qui n'est PAS fait (scope restant)

- CART-3 (CA par catégorie), CART-4 (fraude), CART-5 (stock) : toujours en
  commentaires dans `CartflowTopology.java`.
- Fenêtre volontairement symétrique (pas de `.before()`/`.after()` asymétrique)
  — discutable à l'oral, pas corrigé faute de besoin fonctionnel identifié.
- Pas de grace period ajoutée : on a résolu le problème de fond
  (`max.task.idle.ms`) plutôt que de masquer le symptôme avec un grace period,
  qui aurait été un choix moins correct sémantiquement (un grace period
  suppose des retardataires métier réels, alors que notre souci était un
  artefact de rejeu en masse).

## 9. Questions probables à l'oral (anticipation)

- *"Pourquoi un `selectKey` déclenche un repartitionnement ?"* → réponse
  section 3 (co-partitionnement).
- *"Pourquoi INNER join et pas LEFT ?"* → réponse section 3 — critère
  explicite du README, un `leftJoin` violerait "commande jamais payée ne
  ressort pas".
- *"Vous avez eu des surprises en testant avec des vraies données ?"* →
  section 4, LA bonne histoire à raconter : `max.task.idle.ms`, diagnostic,
  correction, 21% → 98%. Montre une compréhension fine du fonctionnement
  interne de Kafka Streams (stream time, buffers par partition), pas juste
  un copier-coller du DSL.
- *"Ce problème peut arriver en soutenance sur le cluster partagé ?"* → non,
  argumenté : le générateur produit en continu, orders/payments arrivent
  déjà entrelacés dans le temps, `max.task.idle.ms` n'a jamais besoin
  d'attendre longtemps dans ce cas (contrairement au rejeu en masse d'un
  dataset pré-enregistré).
- *"Que devient une commande dont le paiement est DECLINED ?"* → filtrée
  avant même d'entrer dans le repartitionnement/join ; elle ne va ni en DLQ
  (elle est valide au sens CART-1) ni dans `orders.confirmed` — elle
  disparaît silencieusement du flux de sortie, comportement voulu.
- *"Le repartitionnement, montre-le moi."* → `topology.describe()` (loggé
  au démarrage de l'appli), chercher les sinks
  `orders-by-order-id-repartition` et `payments-by-order-id-repartition`
  dans les sous-topologies 0/1, puis la sous-topologie 3 qui les consomme
  pour le join.
