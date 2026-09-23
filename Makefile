# =============================================================
# Fábrica Inteligente - Kafka + Kubernetes
# Execute "make" ou "make help" para ver os alvos disponíveis.
# =============================================================

-include .env
export

NS          ?= fabrinteligente
BROKER      ?= kafka-1-0
BOOTSTRAP   ?= kafka-1-0.kafka-headless:9092
TOPICO      ?= $(TOPICO_SENSORES)
GRUPO       ?= $(GRUPO_CONSUMIDORES)
RENDER_DIR  := k8s/.rendered
# Apenas estas variáveis são substituídas nos manifestos; as demais
# (usadas dentro dos containers) precisam chegar intactas.
SUBST_VARS  := '$$DOCKER_USER $$TAG_SENSOR $$TAG_CONSUMIDOR'

.DEFAULT_GOAL := help

help: ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- Possíveis ajustes necessários ----------

configmap: ## Regenera o ConfigMap a partir do .env
	@grep -vE '^(#|$$|DOCKER_USER=|TAG_|KAFKA_CLUSTER_ID=)' .env \
	 | sed 's/\r$$//' > /tmp/fabrica.env
	kubectl create configmap fabrica-config --from-env-file=/tmp/fabrica.env \
		-n $(NS) --dry-run=client -o yaml > k8s/configmap.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl rollout restart deployment -l app=sensor -n $(NS)
	kubectl rollout restart deployment consumidor -n $(NS)

# ---------- Imagens ----------

build: ## Constrói as imagens Docker localmente
	docker build -t $(DOCKER_USER)/fabrica-sensor:$(TAG_SENSOR) ./produtor
	docker build -t $(DOCKER_USER)/fabrica-consumidor:$(TAG_CONSUMIDOR) ./consumidor

push: ## Publica as imagens no Docker Hub
	docker push $(DOCKER_USER)/fabrica-sensor:$(TAG_SENSOR)
	docker push $(DOCKER_USER)/fabrica-consumidor:$(TAG_CONSUMIDOR)

publicar: build push ## Constrói e publica as imagens

# ---------- Manifestos ----------

render: ## Renderiza os manifestos substituindo DOCKER_USER e as tags
	@mkdir -p $(RENDER_DIR)
	@for f in k8s/*.yaml; do \
		envsubst $(SUBST_VARS) < $$f > $(RENDER_DIR)/$$(basename $$f); \
	done
	@echo "Manifestos renderizados em $(RENDER_DIR)/"

up: render ## Sobe todo o sistema no cluster
	kubectl apply -f $(RENDER_DIR)/namespace.yaml
	kubectl apply -f $(RENDER_DIR)/configmap.yaml
	kubectl apply -f $(RENDER_DIR)/kafka.yaml
	@echo "Aguardando os brokers ficarem prontos..."
	kubectl wait --for=condition=ready pod -l app=kafka -n $(NS) --timeout=180s
	kubectl apply -f $(RENDER_DIR)/criar-topico.yaml
	kubectl wait --for=condition=complete job/criar-topico -n $(NS) --timeout=120s
	kubectl apply -f $(RENDER_DIR)/sensores.yaml
	kubectl apply -f $(RENDER_DIR)/consumidor.yaml
	kubectl apply -f $(RENDER_DIR)/hpa.yaml

down: ## Remove todos os recursos (preserva os volumes)
	kubectl delete namespace $(NS) --ignore-not-found

# ---------- Tópico ----------

topico: ## Mostra partições, líderes e réplicas do tópico
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server $(BOOTSTRAP) --describe --topic $(TOPICO)

offsets: ## Mostra quantas mensagens há em cada partição
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server $(BOOTSTRAP) --topic $(TOPICO)

recriar-topico: ## Apaga e recria o tópico (zera os dados)
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server $(BOOTSTRAP) --delete --topic $(TOPICO)
	@sleep 15
	kubectl delete job criar-topico -n $(NS) --ignore-not-found
	kubectl apply -f $(RENDER_DIR)/criar-topico.yaml

# ---------- Observação ----------

status: ## Visão geral: pods, HPA e uso de recursos
	@kubectl get pods,hpa -n $(NS)
	@echo ""
	@kubectl top pods -n $(NS) 2>/dev/null || echo "(metrics-server ainda coletando)"

grupo: ## Mostra qual consumidor lê qual partição, e o lag
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server $(BOOTSTRAP) --describe --group $(GRUPO)

logs-consumidor: ## Acompanha os logs dos consumidores
	kubectl logs -f -l app=consumidor -n $(NS) --prefix --tail=20

rebalanceamento: ## Mostra apenas os eventos de rebalanceamento
	kubectl logs -l app=consumidor -n $(NS) --tail=-1 --prefix | grep REBALANCO

# ---------- Carga e escala ----------

carga-alta: ## Acelera os sensores para forçar o HPA a escalar
	kubectl patch configmap fabrica-config -n $(NS) \
		-p '{"data":{"INTERVALO_SEGUNDOS":"0.02"}}'
	kubectl rollout restart deployment -l app=sensor -n $(NS)

carga-normal: ## Volta os sensores ao intervalo padrão
	kubectl patch configmap fabrica-config -n $(NS) \
		-p '{"data":{"INTERVALO_SEGUNDOS":"$(INTERVALO_SEGUNDOS)"}}'
	kubectl rollout restart deployment -l app=sensor -n $(NS)

escalar: ## Define o número de consumidores. Uso: make escalar N=3
	kubectl scale deployment consumidor --replicas=$(N) -n $(NS)

# ---------- Testes de falha ----------

falha-broker: ## Simula a queda de um broker e grava as evidências
	./scripts/falha-broker.sh

falha-consumidor: ## Simula a queda de um consumidor e grava as evidências
	./scripts/falha-consumidor.sh

.PHONY: help build push publicar render up down topico offsets recriar-topico \
        status grupo logs-consumidor rebalanceamento carga-alta carga-normal \
        escalar falha-broker falha-consumidor
