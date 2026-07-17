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

## 4. Ce qu'on a trouvé en testant avec des données réelles : `max.task.idle.ms`

**Symptôme** : en rejouant les ~250k messages réels d'un coup
(`make load`), seulement **8 760 commandes confirmées** sur un premier
essai, très en dessous de ce qui semblait attendu. Vérification sur une
paire commande/paiement connue, 20 secondes d'écart, largement dans la
fenêtre de 10 min : **absente** du topic confirmé, et absente aussi de la
DLQ (donc ni rejetée, ni confirmée — juste perdue).

**Cause identifiée** : `max.task.idle.ms` vaut `0` par défaut. Ça veut dire
que Kafka Streams **n'attend pas** d'avoir des données disponibles des deux
côtés d'un join avant de faire avancer son horloge interne ("stream time").
Avec `DataLoader` qui envoie tout un fichier orders d'un coup, un lot de
commandes peut être traité avant que le moindre paiement ne soit lu (le
consumer vide son buffer d'un côté et n'attend pas l'autre) — l'horloge
avance donc sur la base des seules commandes, et referme des fenêtres de
10 min *avant même que le paiement correspondant, pourtant à l'heure, soit
traité*. Avec `NoGrace`, une fenêtre fermée ne rouvre jamais. C'est un
mécanisme réel et bien documenté de Kafka Streams (pas une invention) :
`idle=0` donne systématiquement le résultat le plus bas de tous nos tests.

**Correctif appliqué** : `max.task.idle.ms: 10000` — une valeur positive
raisonnable, nettement meilleure que `0`.

**Honnêteté sur ce qu'on n'a PAS réussi à démontrer** : on a essayé de
mesurer précisément "combien ça s'améliore" en testant plusieurs valeurs
(5s, 10s, 30s, 60s) sur le même jeu de données. Les résultats variaient
fortement (de ~18k à ~40k selon les runs) **y compris entre deux cycles
`reset+init+run+load` strictement identiques** (ex : 20 780 puis 19 930 à
`idle=10s`, deux exécutions consécutives, même config, même données). Une
partie de cette variance venait d'un **process orphelin** qui a pollué
plusieurs de nos mesures sans qu'on le sache sur le moment (voir section 6)
— au point qu'on ne peut plus garantir que notre meilleur chiffre mesuré
(40 276, `idle=60s`) n'était pas lui-même faussé. Au-delà de ce
process fantôme, il reste une variance résiduelle qu'on attribue à des
micro-différences d'ordonnancement système (réseau Docker, disque, GC) —
inhérentes au **rejeu instantané de 250k messages en quelques secondes**,
pas à un défaut de la logique du join (déjà validée indépendamment et de
façon 100% déterministe par les 4 tests `TopologyTestDriver`).

**Conclusion retenue** : `idle=0` est prouvé mauvais et à éviter — c'est le
seul résultat vraiment solide de cette investigation. Au-delà de "positif
vs zéro", chasser une valeur "optimale" via un rejeu instantané en masse
n'est pas une méthode fiable localement. Pour un test/une démo plus
crédible, `make stream` (envoi entrelacé et paced, voir Makefile) donne des
résultats bien plus cohérents (proches du max plausible, toujours inférieurs
au nombre total de paiements donc mathématiquement sains) — c'est la
méthode à privilégier pour montrer CART-2 en conditions réalistes plutôt
que `make load`.

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
- **Un process appli orphelin peut survivre à un `make reset`** : `mvn
  spring-boot:run` lancé en tâche de fond, puis "arrêté", a laissé le
  process Java sous-jacent vivant (le wrapper est mort, pas le vrai
  process). Cette instance zombie continuait de tourner sur le même
  `application-id`/consumer group pendant des cycles de tests censés être
  propres, causant des rebalances et du retraitement à l'insu de tout le
  monde — symptôme repéré via `docker compose down` échouant à supprimer
  des fichiers RocksDB ("Device or resource busy"). Réflexe à garder :
  vérifier `tasklist` (ou équivalent) avant de faire confiance à un test
  "propre", surtout après avoir tué une tâche en arrière-plan.

## 7. Comment on a testé

- **Tests unitaires** (`TopologyTestDriver`, 4 nouveaux cas, pipeInput avec
  `Instant` explicite pour contrôler précisément l'écart temporel) :
  - `commandePayeeSousDixMinEstConfirmee` — paiement à +2 min → confirmée.
  - `commandeJamaisPayeeNeRessortJamais` — aucun paiement → rien ne sort.
  - `paiementHorsFenetreDeDixMinNeConfirmePas` — paiement à +11 min → rien.
  - `paiementDeclineNeConfirmePasLaCommande` — `DECLINED` → filtré avant le join.
- **Validation sur données réelles** (248k messages, `DataLoader`) : voir
  section 4. `idle=0` donne un résultat nettement plus bas que toute valeur
  positive testée (8 760, contre 18k-40k selon la méthode et la valeur) —
  suffisant pour prouver que le fix est nécessaire, insuffisant pour
  garantir un chiffre exact reproductible en rejeu instantané. `make
  stream` (entrelacé) donne des résultats plus cohérents que `make load`
  (instantané) pour ce genre de vérification.

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
- **Reproductibilité de `make load` toujours pas résolue** (voir section 10) :
  le nombre de commandes confirmées reste loin (~50-65%) de la vérité terrain
  calculée par `scripts/cart2_ground_truth.py` (37 766), quel que soit
  `max.task.idle.ms`. Diagnostic actuel : limite inhérente au rejeu massif
  instantané (~15-20s pour 250k messages), pas un bug de la topologie. Pas
  encore validé par un rejeu pacé (`--batch-size`/`--interval-ms` sans
  `--live-timestamps`) — prochaine étape si on reprend ce sujet.

## 9. Questions probables à l'oral (anticipation)

- *"Pourquoi un `selectKey` déclenche un repartitionnement ?"* → réponse
  section 3 (co-partitionnement).
- *"Pourquoi INNER join et pas LEFT ?"* → réponse section 3 — critère
  explicite du README, un `leftJoin` violerait "commande jamais payée ne
  ressort pas".
- *"Vous avez eu des surprises en testant avec des vraies données ?"* →
  section 4, la bonne histoire à raconter : `max.task.idle.ms=0` donne un
  résultat nettement dégradé sur un rejeu massif, diagnostic et correction
  (valeur positive). Montre une compréhension fine du fonctionnement
  interne de Kafka Streams (stream time, buffers par partition) — et
  honnêteté sur les limites de nos mesures en rejeu instantané (variance
  d'un run à l'autre, cause encore partiellement comprise) plutôt qu'un
  chiffre présenté comme définitif sans l'être.
- *"Ce problème peut arriver en soutenance sur le cluster partagé ?"* → non,
  argumenté : le générateur produit en continu, orders/payments arrivent
  déjà entrelacés dans le temps naturellement — `max.task.idle.ms` n'a
  jamais besoin d'attendre longtemps dans ce cas, contrairement au rejeu
  instantané d'un dataset pré-enregistré en quelques secondes (notre
  méthode de test locale, pas les conditions réelles).
- *"Que devient une commande dont le paiement est DECLINED ?"* → filtrée
  avant même d'entrer dans le repartitionnement/join ; elle ne va ni en DLQ
  (elle est valide au sens CART-1) ni dans `orders.confirmed` — elle
  disparaît silencieusement du flux de sortie, comportement voulu.
- *"Le repartitionnement, montre-le moi."* → `topology.describe()` (loggé
  au démarrage de l'appli), chercher les sinks
  `orders-by-order-id-repartition` et `payments-by-order-id-repartition`
  dans les sous-topologies 0/1, puis la sous-topologie 3 qui les consomme
  pour le join.

## 10. Investigation complémentaire (2026-07-17) : `make load` non reproductible

Point de départ : le nombre de commandes confirmées variait à chaque
`reset+init+run+load`, alors qu'on rejoue exactement les mêmes données. La
section 4 avait déjà mesuré ça (20 780 vs 19 930 à `idle=10s`) mais sans
identifier de cause aussi précise que ci-dessous, et sans vérité terrain pour
juger si le résultat était "à peu près bon" ou "loin du compte".

### 10.1 Cause racine trouvée : `DataLoader.replayImmediate` n'entrelaçait pas

`make load` envoyait `cartflow.orders` **en entier**, un `flush()`, puis
`cartflow.payments` **en entier**. Au moment où l'appli traite les premières
commandes, aucun paiement n'existe encore sur le broker : le stream time côté
commandes peut avancer (les timestamps du dataset couvrent des heures) et
fermer des fenêtres de 10 min avant même que les paiements correspondants
n'existent. Contrairement au problème de la section 4, ceci n'est **pas**
une histoire de `max.task.idle.ms` (qui n'aide qu'à attendre une donnée
*déjà présente sur le broker mais pas encore fetchée* — ça n'aide pas quand
la donnée n'a tout simplement pas encore été produite).

**Correctif appliqué** (gardé) : `DataLoader.replayImmediate` entrelace
maintenant les 3 fichiers ligne par ligne (round-robin), en réutilisant
l'abstraction `Cursor` déjà écrite pour `replayStreaming`, au lieu de les
envoyer fichier par fichier. Aucun changement dans `CartflowTopology.java` —
uniquement l'outil de dev, hors scope de notation.

### 10.2 Vérité terrain indépendante : `scripts/cart2_ground_truth.py`

Pour juger si un résultat est "bon" sans se fier à une impression, ce script
recalcule directement sur `data/raw/*.jsonl` (sans Kafka du tout) : les
règles de validation CART-1 (réimplémentation de `Validators.java` en
Python) puis le matching CART-2 (`order_id` + fenêtre ±10 min). Résultat sur
le dataset complet :

```
Orders   : 55201 total, 51971 valides (CART-1)
Payments : 47614 total, 44543 valides (CART-1), 41013 valides+AUTHORIZED
order_id valides matchés dans la fenêtre +/-10min (CART-2) : 37766
```

**Cible de référence : 37 766** commandes confirmées attendues.

### 10.3 Résultats mesurés (4 cycles `reset+init+run+load`, entrelacé)

| `max.task.idle.ms` | Résultat | % de la cible (37 766) |
|---|---|---|
| `0` (mesure de la section 4, avant entrelacement) | 8 760 | 23% |
| `10000` — cycle 1 | 19 715 | 52% |
| `10000` — cycle 2 | 25 118 | 66% |
| `-1` | 6 257 | 17% |
| `60000` | 18 430 | 49% |

### 10.4 Erreur corrigée en cours de route : le sens de `max.task.idle.ms=-1`

Hypothèse de départ (fausse) : `-1` ferait attendre Kafka Streams
indéfiniment. Vérifiée directement dans le bytecode de
`kafka-streams-4.2.1.jar` (`javap` sur `StreamsConfig.class`) :

```
public static final long MAX_TASK_IDLE_MS_DISABLED = -1L;
```

Doc officielle associée : *"Set to -1 to disable idling entirely and process
any locally available data, even though doing so may produce out-of-order
processing."* — `-1` est donc le réglage **le plus agressif** (aucune
attente, même pour une donnée déjà présente sur le broker), pas le plus
patient. Résultat mesuré (6 257, le pire de tous les essais) confirme la
doc. *Bon exemple à citer à l'oral si on demande "avez-vous eu des erreurs
de compréhension corrigées en testant ?" — plutôt que de laisser croire que
tout a été compris du premier coup.*

### 10.5 Conclusion actuelle (sujet non refermé)

Au-delà de `idle=10000`, augmenter encore la valeur (`60000`) n'améliore pas
le résultat — les deux tournent autour de 50-65% de la cible, avec toujours
de la variance entre runs identiques. Diagnostic retenu : `make load`,
même entrelacé, reste un envoi des 250k messages en ~15-20 secondes
réelles ("burst"). Le pipeline a un coût de traversée réel (validation →
topic de repartition → join) ; sur un rejeu aussi compressé, ce délai peut
suffire à rater des fenêtres, indépendamment du réglage `idle`. Cette
hypothèse n'est **pas encore vérifiée** : le test décisif serait un rejeu
pacé sur la totalité du dataset (`--batch-size`/`--interval-ms`, **sans**
`--live-timestamps` pour garder les timestamps originaux du dataset) comparé
à la cible de 37 766 — pas fait faute de temps dans cette session.

**État du code laissé en l'état** : `max.task.idle.ms` remis à `10000`
(aucune valeur testée n'a fait mieux de façon probante, pas de raison de
garder `60000` non justifié). L'entrelacement de `DataLoader` est conservé
(strictement meilleur que l'envoi séquentiel, même si insuffisant seul).
`scripts/cart2_ground_truth.py` reste disponible pour toute future
vérification.

### 10.6 Question probable à l'oral (ajout)

- *"Le nombre de commandes confirmées, vous êtes sûrs qu'il est correct ?"*
  → Non, honnêtement pas encore validé à 100% en local : on sait calculer une
  cible exacte indépendamment de Kafka (37 766, `scripts/cart2_ground_truth.py`),
  et nos rejeux locaux instantanés n'y arrivent qu'à 50-65%, mais on a de
  bonnes raisons de penser (section 10.5) que c'est un artefact du rejeu en
  rafale (~15-20s pour 250k messages) plutôt qu'un bug de la jointure — la
  logique du join elle-même est validée indépendamment et de façon
  déterministe par les tests `TopologyTestDriver` (section 7). Sur le
  cluster partagé, le générateur produit en continu (pas en rafale), donc ce
  problème spécifique ne devrait pas s'y reproduire — mais on n'a pas de
  preuve chiffrée locale pour l'affirmer avec certitude, faute d'avoir
  terminé le test en rejeu pacé.
