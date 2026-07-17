# Force bash pour executer les recettes, peu importe le terminal depuis
# lequel `make` est lance.
SHELL := bash.exe

GROUPE ?= grp01
KAFKA_BOOTSTRAP ?= localhost:29092

# Nombre maximal de messages par fichier.
# Vide = aucune limite.
LIMIT ?=

# Configuration du mode streaming.
BATCH_SIZE ?= 10
INTERVAL_MS ?= 1000

.PHONY: help up wait-kafka topics init run load stream reset down status dlq confirmed

help:
	@echo "Cibles disponibles :"
	@echo ""
	@echo "  make init"
	@echo "      Demarre Kafka et cree les topics."
	@echo ""
	@echo "  make up"
	@echo "      Demarre Kafka et Kafbat UI."
	@echo ""
	@echo "  make run [GROUPE=xxx]"
	@echo "      Lance l'application Spring Boot."
	@echo ""
	@echo "  make load"
	@echo "      Envoie immediatement toutes les donnees."
	@echo ""
	@echo "  make load LIMIT=500"
	@echo "      Envoie au plus 500 lignes par fichier."
	@echo ""
	@echo "  make stream"
	@echo "      Envoie les donnees par batch de 10 toutes les 1000 ms."
	@echo ""
	@echo "  make stream BATCH_SIZE=5 INTERVAL_MS=2000"
	@echo "      Envoie les donnees par batch de 5 toutes les 2 secondes."
	@echo ""
	@echo "  make stream BATCH_SIZE=5 INTERVAL_MS=2000 LIMIT=500"
	@echo "      Mode streaming limite a 500 lignes par fichier."
	@echo ""
	@echo "  make status"
	@echo "      Affiche les containers et le lag Kafka Streams."
	@echo ""
	@echo "  make dlq"
	@echo "      Affiche les 20 premiers messages du DLQ."
	@echo ""
	@echo "  make confirmed"
	@echo "      Affiche les 20 premiers messages de orders.confirmed."
	@echo ""
	@echo "  make fraud"
	@echo "      Affiche les alertes de fraude."
	@echo ""
	@echo "  make stock"
	@echo "      Affiche les 20 premieres mises a jour des niveaux de stock."
	@echo ""
	@echo "  make alerts-stock"
	@echo "      Affiche les 20 premieres alertes de rupture."
	@echo ""
	@echo "  make reset"
	@echo "      Supprime le cluster, les volumes et le dossier target."

## ---------------------------------------------------------------------------
## Demarrage du cluster
## ---------------------------------------------------------------------------

up:
	docker compose up -d

wait-kafka:
	@echo "Attente que le cluster Kafka reponde..."
	@until MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--list >/dev/null 2>&1; do \
		sleep 2; \
	done
	@echo "Cluster pret."

topics:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn -q compile exec:java \
		-Dexec.args="--topics-only"

init: up wait-kafka topics
	@echo ""
	@echo "Initialisation terminee."
	@echo "Etapes suivantes :"
	@echo "  1. make run"
	@echo "  2. make load ou make stream dans un autre terminal"

## ---------------------------------------------------------------------------
## Application Spring Boot
## ---------------------------------------------------------------------------

run:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn spring-boot:run

## ---------------------------------------------------------------------------
## Chargement immediat
## ---------------------------------------------------------------------------

# Tout envoyer :
#   make load
#
# Limiter le nombre de messages par fichier :
#   make load LIMIT=500
load:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn -q compile exec:java \
		$(if $(LIMIT),-Dexec.args="--limit $(LIMIT)",)

## ---------------------------------------------------------------------------
## Simulation streaming
## ---------------------------------------------------------------------------

# Exemples :
#
#   make stream
#   make stream BATCH_SIZE=5 INTERVAL_MS=2000
#   make stream BATCH_SIZE=5 INTERVAL_MS=2000 LIMIT=500
#
# --live-timestamps utilise l'heure actuelle comme timestamp Kafka.
stream:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn -q compile exec:java \
		-Dexec.args="--batch-size $(BATCH_SIZE) --interval-ms $(INTERVAL_MS) --live-timestamps $(if $(LIMIT),--limit $(LIMIT),)"

## ---------------------------------------------------------------------------
## Observabilite
## ---------------------------------------------------------------------------

status:
	@echo "=== Containers Docker ==="
	@docker ps --format "table {{.Names}}\t{{.Status}}"
	@echo ""
	@echo "=== Lag du consumer group cartflow-$(GROUPE) ==="
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--describe \
		--group cartflow-$(GROUPE) 2>&1 | \
		grep -v "^Note:" || true

dlq:
	@echo "(20 premiers messages max - le topic peut en contenir davantage)"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.dlq \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 15000 \
		--property print.key=true 2>&1 | \
		grep -v "^\["

confirmed:
	@echo "(20 premiers messages max - le topic peut en contenir davantage)"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.orders.confirmed \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 15000 \
		--property print.key=true 2>&1 | \
		grep -v "^\["

fraud:
	@echo "(20 premieres alertes de fraude)"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.alerts.fraud \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 15000 \
		--property print.key=true 2>&1 | \
		grep -v "^\["

stock:
	@echo "(20 premieres mises a jour des niveaux de stock)"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.stock.levels \
		--from-beginning --max-messages 20 --timeout-ms 15000 \
		--property print.key=true 2>&1 | grep -v "^\["

alerts-stock:
	@echo "(20 premieres alertes de rupture)"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.alerts.stock \
		--from-beginning --max-messages 20 --timeout-ms 15000 \
		--property print.key=true 2>&1 | grep -v "^\["
## ---------------------------------------------------------------------------
## Nettoyage
## ---------------------------------------------------------------------------

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans
	mvn -q clean
	@rm -rf "$$TEMP/kafka-streams/cartflow-$(GROUPE)"
	@echo "Reset complet : volumes, topics, messages, target et etat RocksDB local supprimes."