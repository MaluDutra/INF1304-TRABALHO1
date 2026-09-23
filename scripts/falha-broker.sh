#!/bin/bash
#
# Simula a falha de um broker Kafka e registra as evidências.
#
# O teste demonstra duas propriedades distintas:
#   1. Tolerância a falhas do Kafka: com replicação 3 e
#      min.insync.replicas=2, o cluster continua operando com
#      apenas 2 brokers, elegendo novos líderes automaticamente.
#   2. Self-healing do Kubernetes: o StatefulSet recria o pod
#      derrubado sem intervenção manual.
#
# Uso: ./scripts/falha-broker.sh [nome-do-pod]
#
set -euo pipefail

NAMESPACE="${NAMESPACE:-fabrinteligente}"
TOPICO="${TOPICO_SENSORES:-dados-sensores}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1-0.kafka-headless:9092}"
BROKER_ALVO="${1:-kafka-2-0}"
BROKER_OBSERVADOR="kafka-1-0"

CARIMBO=$(date +%Y%m%d-%H%M%S)
SAIDA="logs/falha-broker-${CARIMBO}.log"
mkdir -p logs

# Registra simultaneamente no terminal e no arquivo de log
registrar() {
    echo "$@" | tee -a "$SAIDA"
}

# Executa o kafka-topics.sh dentro de um broker que permanece no ar
descrever_topico() {
    kubectl exec -n "$NAMESPACE" "$BROKER_OBSERVADOR" -- \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --describe --topic "$TOPICO" 2>&1 | tee -a "$SAIDA"
}

registrar "=========================================================="
registrar " TESTE DE FALHA DE BROKER - $(date)"
registrar " Broker alvo: $BROKER_ALVO"
registrar "=========================================================="
registrar ""
registrar "--- 1. ESTADO INICIAL ---"
registrar "Esperado: Isr com os tres brokers em todas as particoes"
registrar ""
descrever_topico

registrar ""
registrar "--- 2. DERRUBANDO O BROKER $BROKER_ALVO ---"
kubectl delete pod "$BROKER_ALVO" -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"

registrar ""
registrar "--- 3. ESTADO DURANTE A FALHA ---"
registrar "Esperado: Isr reduzido a dois brokers e novos lideres eleitos."
registrar "O cluster responde normalmente, provando a tolerancia a falha."
registrar ""
descrever_topico

registrar ""
registrar "--- 4. PODS DURANTE A RECUPERACAO ---"
kubectl get pods -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"

registrar ""
registrar "--- 5. AGUARDANDO O KUBERNETES RECRIAR O POD (ate 120s) ---"
kubectl wait --for=condition=ready "pod/$BROKER_ALVO" \
    -n "$NAMESPACE" --timeout=120s 2>&1 | tee -a "$SAIDA"

registrar ""
registrar "--- 6. AGUARDANDO A RESSINCRONIZACAO DAS REPLICAS (30s) ---"
sleep 30

registrar ""
registrar "--- 7. ESTADO FINAL ---"
registrar "Esperado: Isr novamente com os tres brokers."
registrar "Observacao: o broker que voltou nao retoma automaticamente a"
registrar "lideranca das particoes; isso exige eleicao explicita ou a"
registrar "opcao auto.leader.rebalance.enable."
registrar ""
descrever_topico

registrar ""
registrar "--- 8. SENSORES E CONSUMIDORES CONTINUARAM ATIVOS? ---"
kubectl get pods -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"

registrar ""
registrar "Evidencias salvas em: $SAIDA"
