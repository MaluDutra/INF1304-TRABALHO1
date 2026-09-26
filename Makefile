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
# Persistência: valores de reserva caso o .env não exista
DIR_DADOS       ?= /dados
ARQUIVO_DADOS   ?= dados-processados.jsonl
ARQUIVO_ALERTAS ?= alertas.jsonl
# Qualquer réplica serve: todas montam o mesmo volume
# Os arquivos só são acessíveis por dentro de um consumidor, que é quem monta o volume.
# Sem nenhum rodando, o alvo para com uma mensagem clara em vez do erro do jsonpath.
POD_CONSUMIDOR  := pod=$$(kubectl get pod -l app=consumidor -n $(NS) \
                   --field-selector=status.phase=Running \
                   -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); \
                   [ -n "$$pod" ] || { echo "Nenhum consumidor rodando: suba um com 'make escalar N=1'"; exit 1; }
# Apenas estas variáveis são substituídas nos manifestos; as demais
# (usadas dentro dos containers) precisam chegar intactas.
SUBST_VARS  := '$$DOCKER_USER $$TAG_SENSOR $$TAG_CONSUMIDOR $$KAFKA_CLUSTER_ID \
                $$KAFKA_IMAGEM $$PARTICOES $$REPLICACAO $$MIN_ISR $$MAX_CONSUMIDORES \
                $$HPA_CPU_ALVO'

TIMEOUT_CLI_MS  ?= 30000

.DEFAULT_GOAL := help

help: ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- Possíveis ajustes necessários ----------

configmap: ## Regenera o ConfigMap a partir do .env
	@grep -vE '^(#|$$|DOCKER_USER=|TAG_|KAFKA_CLUSTER_ID=|KAFKA_IMAGEM=|MIN_ISR=|MAX_CONSUMIDORES=|HPA_CPU_ALVO=|TIMEOUT_CLI_MS=)' .env \
	 | sed 's/\r$$//' > /tmp/fabrica.env
	kubectl create configmap fabrica-config --from-env-file=/tmp/fabrica.env \
		-n $(NS) --dry-run=client -o yaml > k8s/configmap.yaml
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	@# Na primeira instalação ainda não há pods para reiniciar: o make up já sobe
	@# tudo lendo o ConfigMap novo
	@if kubectl get deployment consumidor -n $(NS) >/dev/null 2>&1; then \
		kubectl rollout restart deployment -l app=sensor -n $(NS); \
		kubectl rollout restart deployment consumidor -n $(NS); \
	else \
		echo "Sistema ainda não está no ar: o ConfigMap será usado no próximo make up"; \
	fi

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

metrics-server: ## Instala o metrics-server (necessário para o HPA no Docker Desktop)
	kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
	@kubectl get deployment metrics-server -n kube-system -o jsonpath='{.spec.template.spec.containers[0].args}' \
	 | grep -q kubelet-insecure-tls \
	 || kubectl patch deployment metrics-server -n kube-system --type=json \
	 -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
	kubectl rollout status deployment metrics-server -n kube-system --timeout=120s

up: render metrics-server ## Sobe todo o sistema no cluster
	kubectl apply -f $(RENDER_DIR)/namespace.yaml
	kubectl apply -f $(RENDER_DIR)/configmap.yaml
	kubectl apply -f $(RENDER_DIR)/pvc-dados.yaml
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

# O Kafka só aceita mudar os offsets de um grupo sem membros ativos: por isso os
# consumidores são parados antes e religados depois. Com zero réplicas o HPA fica
# suspenso, e volta a atuar quando o Deployment é escalado para 1 de novo.
resetar-offsets: ## Pula as mensagens acumuladas: leva o grupo para o fim do tópico
	kubectl scale deployment consumidor --replicas=0 -n $(NS)
	-kubectl wait --for=delete pod -l app=consumidor -n $(NS) --timeout=120s
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server $(BOOTSTRAP) --group $(GRUPO) --topic $(TOPICO) \
		--reset-offsets --to-latest --execute --timeout $(TIMEOUT_CLI_MS)
	kubectl scale deployment consumidor --replicas=1 -n $(NS)

# ---------- Observação ----------

status: ## Visão geral: pods, HPA e uso de recursos
	@kubectl get pods,hpa -n $(NS)
	@echo ""
	@kubectl top pods -n $(NS) 2>/dev/null || echo "(metrics-server ainda coletando)"

grupo: ## Mostra qual consumidor lê qual partição, e o lag
	kubectl exec -n $(NS) $(BROKER) -- /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server $(BOOTSTRAP) --describe --group $(GRUPO) \
		--timeout $(TIMEOUT_CLI_MS)

logs-sensores: ## Acompanha os logs dos sensores
	kubectl logs -f -l app=sensor -n $(NS) --prefix --tail=20 --max-log-requests=10

logs-consumidor: ## Acompanha os logs dos consumidores
	kubectl logs -f -l app=consumidor -n $(NS) --prefix --tail=20

rebalanceamento: ## Mostra apenas os eventos de rebalanceamento
	kubectl logs -l app=consumidor -n $(NS) --tail=-1 --prefix | grep REBALANCO

# ---------- Dados persistidos ----------

# Várias réplicas gravam nos mesmos dois arquivos. Se o append atômico estiver
# correto, nenhuma linha fica truncada ou misturada com a de outro consumidor:
# é exatamente isso que este script verifica.
define VALIDAR_PY
import json, sys
houve_falha = False
for caminho in sys.argv[1:]:
    total = ruins = 0
    for n, linha in enumerate(open(caminho, encoding="utf-8"), 1):
        total += 1
        try:
            json.loads(linha)
        except json.JSONDecodeError as erro:
            ruins += 1
            if ruins <= 3:
                print(f"   linha {n} corrompida: {erro}")
    print(f"{caminho}: {total} linhas, {ruins} corrompidas")
    houve_falha = houve_falha or ruins > 0
sys.exit(1 if houve_falha else 0)
endef
export VALIDAR_PY

dados: ## Copia os dados processados e os alertas do volume para ./dados/
	@mkdir -p dados
	@$(POD_CONSUMIDOR); \
	 kubectl exec -n $(NS) $$pod -- cat $(DIR_DADOS)/$(ARQUIVO_DADOS)   > dados/$(ARQUIVO_DADOS); \
	 kubectl exec -n $(NS) $$pod -- cat $(DIR_DADOS)/$(ARQUIVO_ALERTAS) > dados/$(ARQUIVO_ALERTAS)
	@wc -l dados/$(ARQUIVO_DADOS) dados/$(ARQUIVO_ALERTAS)

alertas: ## Mostra os últimos alertas gravados, sem copiar nada
	@$(POD_CONSUMIDOR); \
	 kubectl exec -n $(NS) $$pod -- tail -n 20 $(DIR_DADOS)/$(ARQUIVO_ALERTAS)

validar-dados: ## Verifica que toda linha é um JSON válido (prova do append atômico)
	@python3 -c "$$VALIDAR_PY" dados/$(ARQUIVO_DADOS) dados/$(ARQUIVO_ALERTAS)

dados-resumo: dados ## Conta os registros por consumidor e por partição (evidência do balanceamento)
	@echo ""
	@echo "== Registros por consumidor =="
	@grep -o '"consumidor":"[^"]*"' dados/$(ARQUIVO_DADOS) | cut -d'"' -f4 | sort | uniq -c
	@echo ""
	@echo "== Registros por partição =="
	@grep -o '"particao":[0-9]*' dados/$(ARQUIVO_DADOS) | cut -d: -f2 | sort -n | uniq -c
	@echo ""
	@echo "== Alertas por parâmetro =="
	@grep -o '"parametro":"[^"]*"' dados/$(ARQUIVO_ALERTAS) | cut -d'"' -f4 | sort | uniq -c
	@echo ""
	@echo "== Leituras duplicadas (particao,offset) =="
	@echo "   esperadas após um rebalanço: é a semântica at-least-once em ação"
	@grep -o '"particao":[0-9]*,"offset":[0-9]*' dados/$(ARQUIVO_DADOS) \
	 | sort | uniq -d | wc -l

dashboard: ## Gera o dashboard HTML a partir dos dados ja coletados
	python3 dashboard/gerar_dashboard.py

limpar-dados: ## Esvazia os arquivos no volume para começar um teste limpo
	@$(POD_CONSUMIDOR); \
	 kubectl exec -n $(NS) $$pod -- sh -c \
	   ': > $(DIR_DADOS)/$(ARQUIVO_DADOS); : > $(DIR_DADOS)/$(ARQUIVO_ALERTAS)'
	@echo "Arquivos de dados e de alertas esvaziados"

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

# Alternativa ao carga-alta: em vez de acelerar os sensores, aumenta o número de
# produtores. Escalando TODAS as máquinas, as chaves continuam sendo as 6 de sempre
# e a carga extra se espalha igualmente pelas 3 partições.
escalar-sensores: ## Réplicas de todas as máquinas. Uso: make escalar-sensores N=10
	kubectl scale deployment -l app=sensor --replicas=$(N) -n $(NS)

# Réplicas de UMA máquina compartilham o SENSOR_ID, que é a chave da mensagem:
# todas caem na mesma partição, e só um consumidor recebe a carga extra.
escalar-maquina: ## Réplicas de uma máquina só. Uso: make escalar-maquina MAQUINA=trn N=10
	kubectl scale deployment sensor-$(MAQUINA) --replicas=$(N) -n $(NS)

watch-hpa: ## Acompanha o HPA escalando os consumidores em tempo real
	watch -n 2 'kubectl get hpa,pods -n $(NS) -l app=consumidor'

# ---------- Testes de falha ----------

falha-broker: ## Simula a queda de um broker e grava as evidências
	./scripts/falha-broker.sh

falha-consumidor: ## Simula a queda de um consumidor e grava as evidências
	./scripts/falha-consumidor.sh

.PHONY: help build push publicar render metrics-server up down topico offsets recriar-topico resetar-offsets \
        status grupo logs-sensores logs-consumidor rebalanceamento dados alertas \
        validar-dados dados-resumo dashboard limpar-dados carga-alta carga-normal escalar \
        watch-hpa escalar-sensores escalar-maquina falha-broker falha-consumidor
