# Commandes du projet CARTFLOW. `make help` pour la liste.
#
# MSYS_NO_PATHCONV evite que Git Bash convertisse les chemins Unix
# (ex: /opt/kafka/bin/...) passes a `docker exec` en chemins Windows.
export MSYS_NO_PATHCONV = 1

GROUPE ?= grp01
KAFKA_BOOTSTRAP ?= localhost:29092
LIMIT ?=

.PHONY: help up wait-kafka topics init run load reset down status dlq

help:
	@echo "Cibles disponibles :"
	@echo "  make init              - 1ere fois : cluster Docker + attente + topics (source + DLQ)"
	@echo "  make up                - demarre juste le cluster Docker (Kafka + Kafbat UI)"
	@echo "  make run [GROUPE=xxx]  - lance l'appli Spring Boot (GROUPE=grp01 par defaut)"
	@echo "  make load              - envoie data/raw/*.jsonl EN ENTIER sur les topics source"
	@echo "  make load LIMIT=500    - envoie au plus 500 lignes par fichier"
	@echo "  make status            - etat des containers + lag du consumer group"
	@echo "  make dlq                - affiche le contenu de <GROUPE>.cartflow.dlq"
	@echo "  make reset             - stoppe/supprime le cluster Docker (efface tout, y compris l'UI) + mvn clean"

## --- Demarrage ---

up:
	docker compose up -d

wait-kafka:
	@echo "Attente que le cluster Kafka reponde..."
	@until docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do \
		sleep 2; \
	done
	@echo "Cluster pret."

topics:
	mvn -q exec:java -Dexec.args="--limit 0"

init: up wait-kafka topics
	@echo ""
	@echo "Init terminee : cluster + topics (source + DLQ) prets."
	@echo "Etape suivante : make run   (dans un autre terminal), puis make load"

## --- Appli et donnees ---

run:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) mvn spring-boot:run

load:
	mvn -q exec:java $(if $(LIMIT),-Dexec.args="--limit $(LIMIT)",)

## --- Observabilite ---

status:
	@echo "=== Containers Docker ==="
	@docker ps --format "table {{.Names}}\t{{.Status}}"
	@echo ""
	@echo "=== Lag du consumer group cartflow-$(GROUPE) ==="
	@docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --describe --group cartflow-$(GROUPE) 2>&1 | grep -v "^Note:" || true

dlq:
	docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic $(GROUPE).cartflow.dlq \
		--from-beginning --timeout-ms 5000 --property print.key=true 2>&1 | grep -v "^\["

## --- Nettoyage ---

down:
	docker compose down

reset: down
	mvn -q clean
	@echo "Reset complet : cluster Docker (Kafka + Kafbat UI, donc tous les topics/messages) supprime, target/ nettoye."
