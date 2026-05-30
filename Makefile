# Socle Data Kafka — commandes du lab et de la CI.  `make help` pour la liste.
COMPOSE_DIR := platform/docker-compose
COMPOSE     := docker compose -f $(COMPOSE_DIR)/docker-compose.yml --env-file $(COMPOSE_DIR)/.env
MVN         := mvn -B -f services/pom.xml

.DEFAULT_GOAL := help
.PHONY: help env build images up down start stop health demo logs generate check test clean-volumes

help: ## Affiche cette aide
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## Crée les fichiers locaux .env et secrets du lab s'ils n'existent pas
	@test -f $(COMPOSE_DIR)/.env || cp $(COMPOSE_DIR)/.env.example $(COMPOSE_DIR)/.env
	@test -f $(COMPOSE_DIR)/config/connect-secrets.properties || cp $(COMPOSE_DIR)/config/connect-secrets.properties.example $(COMPOSE_DIR)/config/connect-secrets.properties

build: ## Compile et teste les services Java
	$(MVN) verify

images: env ## Construit les images (services + Kafka Connect)
	$(MVN) package -DskipTests
	$(COMPOSE) build

up: env images ## Démarre tout le lab (ordre géré par depends_on)
	$(COMPOSE) up -d
	@echo "Grafana http://localhost:3000 · Kafka UI http://localhost:8080 · API http://localhost:8090/swagger-ui.html"

start: env ## Relance ordonnée et contrôlée (PROC-03)
	scripts/ops/start-platform.sh

stop: ## Arrêt ordonné, supervision conservée (PROC-02)
	scripts/ops/stop-platform.sh

down: ## Arrêt ordonné puis suppression des conteneurs (les volumes sont conservés)
	scripts/ops/stop-platform.sh --with-monitoring
	$(COMPOSE) down

health: ## Contrôle de santé (PROC-01)
	scripts/ops/healthcheck.sh

demo: ## Envoie des commandes de test et montre le résultat
	scripts/ops/demo.sh

logs: ## Suit les journaux (make logs S=broker-1)
	$(COMPOSE) logs -f $(S)

generate: ## Régénère les fichiers « as code » (topics, tableaux de bord, Strimzi)
	python3 tools/topics.py
	python3 tools/gen_dashboards.py
	python3 tools/sync_strimzi_monitoring.py

check: ## Vérifications de la CI exécutables en local
	python3 tools/topics.py --check
	python3 tools/gen_dashboards.py --check
	python3 tools/sync_strimzi_monitoring.py --check
	python3 tools/register_schemas.py --resolve-only > /dev/null
	python3 tools/check_links.py
	yamllint -s .
	shellcheck -x --source-path=SCRIPTDIR -S warning scripts/ops/*.sh governance/generated/create-topics.sh

test: build ## Alias de build

clean-volumes: ## DANGER : supprime toutes les données du lab
	$(COMPOSE) down -v
