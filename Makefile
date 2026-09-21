# Makefile da Fábrica Inteligente
# Comandos para gerenciar o sistema distribuído (Kafka, sensores e consumidores)

# Carrega as variáveis do .env (TOPICO_SENSORES, PARTICOES, REPLICACAO, GRUPO_CONSUMIDORES...)
-include .env
export

KAFKA_CLI = /opt/kafka/bin
BROKER_INTERNO = kafka1:19092
BROKERS = kafka1 kafka2 kafka3
# Serviço único do consumidor no docker-compose.yml, escalado em réplicas (todas no mesmo grupo)
CONSUMIDOR = consumidor
NUM_CONSUMIDORES ?= 3
# Broker derrubado por falha-broker (ex.: make falha-broker BROKER=kafka3)
BROKER ?= kafka2

.PHONY: help setup start stop restart clean status health logs topics esperar-kafka \
	construir-consumidores iniciar-consumidores parar-consumidores reiniciar-consumidores \
	logs-consumidores grupo-consumidores escalar falha-consumidor falha-broker

# Alvo padrão
help:
	@echo "Fábrica Inteligente - Comandos disponíveis:"
	@echo ""
	@echo "Geral:"
	@echo "  setup             	     - Verifica se o arquivo .env existe"
	@echo "  start             	     - Inicia o cluster Kafka e cria os tópicos"
	@echo "  stop             	     - Para todos os serviços"
	@echo "  restart	      	     - Reinicia todos os serviços"
	@echo "  clean            	     - Remove containers e volumes do projeto"
	@echo "  status           	     - Mostra o estado dos serviços"
	@echo "  health           	     - Verifica a saúde dos brokers Kafka"
	@echo "  logs             	     - Mostra os logs agregados"
	@echo "  logs-<serviço>   	     - Mostra os logs de um serviço (ex.: logs-kafka1)"
	@echo "  topics           	     - Cria os tópicos do Kafka"
	@echo ""
	@echo "Consumidores:"
	@echo "  construir-consumidores - Constrói a imagem dos consumidores"
	@echo "  iniciar-consumidores   - Inicia os consumidores (NUM_CONSUMIDORES=3 réplicas por padrão)"
	@echo "  parar-consumidores     - Para os consumidores"
	@echo "  reiniciar-consumidores - Reconstrói e reinicia os consumidores"
	@echo "  logs-consumidores      - Mostra os logs dos consumidores"
	@echo "  grupo-consumidores     - Mostra as partições e o lag do grupo de consumidores"
	@echo "  escalar N=<n>          - Altera o número de réplicas sem recriar as existentes"
	@echo ""
	@echo "Simulação de falhas:"
	@echo "  falha-consumidor       - Derruba uma réplica do consumidor (rebalanço)"
	@echo "  falha-broker           - Derruba um broker Kafka (BROKER=kafka2 por padrão)"

# Verifica se o ambiente está configurado
setup:
	@if [ ! -f .env ]; then echo "Erro: arquivo .env não encontrado!"; exit 1; fi
	@echo "Ambiente configurado com sucesso!"

# Inicia o cluster Kafka e cria os tópicos
start: setup
	@echo "Iniciando o cluster Kafka..."
	@docker compose up -d $(BROKERS)
	@$(MAKE) --no-print-directory esperar-kafka
	@$(MAKE) --no-print-directory topics
	@echo "Cluster Kafka iniciado com sucesso!"

# Para todos os serviços
stop:
	@echo "Parando o sistema..."
	@docker compose down
	@echo "Sistema parado com sucesso!"

# Reinicia todos os serviços
restart: stop start

# Remove containers, redes e volumes do projeto
clean:
	@echo "Isto removerá os containers, redes e volumes do projeto!"
	@read -p "Tem certeza? [s/N] " confirmacao && [ "$$confirmacao" = "s" ] || exit 1
	@docker compose down -v --remove-orphans
	@echo "Limpeza concluída!"

# Mostra o estado dos serviços
status:
	@echo "Estado dos serviços:"
	@echo "===================="
	@docker compose ps

# Verifica a saúde dos brokers Kafka
health:
	@echo "Verificando a saúde dos brokers..."
	@echo "=================================="
	@for broker in $(BROKERS); do \
		docker compose exec -T $$broker $(KAFKA_CLI)/kafka-broker-api-versions.sh --bootstrap-server $$broker:19092 >/dev/null 2>&1 \
			&& echo "✓ $$broker: saudável" || echo "✗ $$broker: indisponível"; \
	done

# Mostra os logs agregados
logs:
	@echo "Mostrando os logs do sistema (Ctrl+C para sair)..."
	@docker compose logs -f

# Mostra os logs de um serviço específico
logs-%:
	@echo "Mostrando os logs de $* (Ctrl+C para sair)..."
	@docker compose logs -f --tail=100 $*

# Cria os tópicos do Kafka
topics:
	@echo "Criando o tópico $(TOPICO_SENSORES)..."
	@docker compose exec -T kafka1 $(KAFKA_CLI)/kafka-topics.sh \
		--bootstrap-server $(BROKER_INTERNO) \
		--create \
		--topic $(TOPICO_SENSORES) \
		--partitions $(PARTICOES) \
		--replication-factor $(REPLICACAO) \
		--if-not-exists
	@echo "Tópicos existentes:"
	@docker compose exec -T kafka1 $(KAFKA_CLI)/kafka-topics.sh --bootstrap-server $(BROKER_INTERNO) --list

# Espera os brokers Kafka ficarem prontos
esperar-kafka:
	@echo "Aguardando os brokers Kafka ficarem prontos..."
	@for i in $$(seq 1 60); do \
		pronto=1; \
		for broker in $(BROKERS); do \
			docker compose exec -T $$broker $(KAFKA_CLI)/kafka-broker-api-versions.sh --bootstrap-server $$broker:19092 >/dev/null 2>&1 || pronto=0; \
		done; \
		if [ $$pronto -eq 1 ]; then \
			echo "✓ Todos os brokers Kafka estão prontos!"; \
			exit 0; \
		fi; \
		echo "Brokers Kafka iniciando... ($$i/60)"; \
		sleep 2; \
	done; \
	echo "✗ Tempo esgotado aguardando os brokers Kafka"; \
	$(MAKE) --no-print-directory health; \
	exit 1

# Constrói a imagem dos consumidores
construir-consumidores:
	@echo "Construindo a imagem dos consumidores..."
	@docker compose build $(CONSUMIDOR)
	@echo "Imagem dos consumidores construída com sucesso!"

# Inicia os consumidores (réplicas do serviço, ex.: make iniciar-consumidores NUM_CONSUMIDORES=2)
iniciar-consumidores:
	@echo "Iniciando $(NUM_CONSUMIDORES) consumidores..."
	@docker compose up -d --scale $(CONSUMIDOR)=$(NUM_CONSUMIDORES) $(CONSUMIDOR)
	@echo "Consumidores iniciados com sucesso!"

# Para os consumidores
parar-consumidores:
	@echo "Parando os consumidores..."
	@docker compose stop $(CONSUMIDOR)
	@echo "Consumidores parados!"

# Reconstrói e reinicia os consumidores (útil após alterar o processador.py)
reiniciar-consumidores: parar-consumidores construir-consumidores iniciar-consumidores

# Mostra os logs de todas as réplicas dos consumidores
logs-consumidores:
	@echo "Mostrando os logs dos consumidores (Ctrl+C para sair)..."
	@docker compose logs -f --tail=100 $(CONSUMIDOR)

# Mostra as partições atribuídas a cada consumidor e o lag do grupo
grupo-consumidores:
	@echo "Grupo de consumidores: $(GRUPO_CONSUMIDORES)"
	@docker compose exec -T kafka1 $(KAFKA_CLI)/kafka-consumer-groups.sh \
		--bootstrap-server $(BROKER_INTERNO) \
		--describe \
		--group $(GRUPO_CONSUMIDORES)

# Altera o número de réplicas sem recriar as existentes (ex.: make escalar N=5)
# O --no-recreate mantém as réplicas vivas, então o log mostra só o rebalanço
escalar:
	@if [ -z "$(N)" ]; then echo "Erro: informe o número de réplicas (ex.: make escalar N=5)"; exit 1; fi
	@echo "Escalando $(CONSUMIDOR) para $(N) réplicas..."
	@docker compose up -d --no-recreate --scale $(CONSUMIDOR)=$(N) $(CONSUMIDOR)
	@echo "Escala ajustada!"

# Derruba uma réplica do consumidor para demonstrar o rebalanço
falha-consumidor:
	@alvo=$$(docker compose ps -q $(CONSUMIDOR) | head -1); \
	if [ -z "$$alvo" ]; then echo "Erro: nenhuma réplica de $(CONSUMIDOR) em execução"; exit 1; fi; \
	echo "Derrubando o consumidor $$(docker inspect -f '{{.Config.Hostname}}' $$alvo)..."; \
	docker stop $$alvo >/dev/null; \
	echo "Consumidor derrubado! Acompanhe com: make logs-consumidores"

# Derruba um broker Kafka para demonstrar o failover (ex.: make falha-broker BROKER=kafka3)
falha-broker:
	@echo "Derrubando o broker $(BROKER)..."
	@docker stop $(BROKER) >/dev/null
	@echo "Broker derrubado! Verifique com: make health"
