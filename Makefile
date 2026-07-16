# Force bash pour executer les recettes, peu importe le terminal depuis
# lequel `make` est lance (PowerShell/cmd utiliseraient sinon cmd.exe, qui
# ne comprend pas la syntaxe bash utilisee ici : until/done, /dev/null...).
SHELL := bash.exe

GROUPE ?= grp01
KAFKA_BOOTSTRAP ?= localhost:29092
LIMIT ?=

.PHONY: help up wait-kafka topics init run load reset down status dlq confirmed

help:
	@echo "Cibles disponibles :"
	@echo "  make init              - cluster Docker + attente + creation des topics"
	@echo "  make up                - demarre Kafka et Kafbat UI"
	@echo "  make run [GROUPE=xxx]  - lance l'application Spring Boot"
	@echo "  make load              - charge toutes les donnees"
	@echo "  make load LIMIT=500    - charge au plus 500 lignes par fichier"
	@echo "  make status            - affiche les containers et le lag"
	@echo "  make dlq               - affiche le contenu du DLQ"
	@echo "  make confirmed         - affiche le contenu de orders.confirmed (CART-2)"
	@echo "  make reset             - supprime le cluster et nettoie target"

## --- Demarrage ---

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
		mvn -q compile exec:java -Dexec.args="--topics-only"

init: up wait-kafka topics
	@echo ""
	@echo "Init terminee."
	@echo "Etape suivante : make run, puis make load dans un autre terminal."

## --- Application et donnees ---

run:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn spring-boot:run

load:
	GROUPE=$(GROUPE) KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
		mvn -q compile exec:java \
		$(if $(LIMIT),-Dexec.args="--limit $(LIMIT)",)

## --- Observabilite ---

status:
	@echo "=== Containers Docker ==="
	@docker ps --format "table {{.Names}}\t{{.Status}}"
	@echo ""
	@echo "=== Lag du consumer group cartflow-$(GROUPE) ==="
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--describe \
		--group cartflow-$(GROUPE) 2>&1 | grep -v "^Note:" || true

dlq:
	@echo "(20 premiers messages max - le topic peut en contenir bien plus, voir 'make status')"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.dlq \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 15000 \
		--property print.key=true 2>&1 | grep -v "^\["

confirmed:
	@echo "(20 premiers messages max - le topic peut en contenir bien plus, voir 'make status')"
	@MSYS_NO_PATHCONV=1 docker exec kafka-1 \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(GROUPE).cartflow.orders.confirmed \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 15000 \
		--property print.key=true 2>&1 | grep -v "^\["

## --- Nettoyage ---

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans
	mvn -q clean
	@echo "Reset complet : volumes, topics, messages et target supprimes."
