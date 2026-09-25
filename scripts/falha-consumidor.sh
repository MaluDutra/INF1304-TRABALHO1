#!/bin/bash
#
# Simula a falha de um consumidor e registra o rebalanceamento.
#
# O teste demonstra:
#   1. Balanceamento de carga: com 3 consumidores no mesmo grupo e
#      3 partições, cada um recebe exatamente uma partição.
#   2. Rebalanceamento automático: ao perder um consumidor, o
#      coordenador redistribui as partições órfãs entre os que
#      restaram, sem intervenção manual e sem perda de mensagens.
#   3. Self-healing do Kubernetes: o Deployment recria o pod, que
#      reingressa no grupo e dispara um novo rebalanceamento.
#
# Uso: ./scripts/falha-consumidor.sh
#
set -euo pipefail

NAMESPACE="${NAMESPACE:-fabrinteligente}"
GRUPO="${GRUPO_CONSUMIDORES:-processadores}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1-0.kafka-headless:9092}"
BROKER="kafka-1-0"
REPLICAS_TESTE="${MAX_CONSUMIDORES:-3}"
# O padrao de 5s da kafka-consumer-groups.sh estoura antes de encontrar o
# coordenador: a CLI sobe uma JVM dentro do broker e disputa a CPU dele.
TIMEOUT_CLI="${TIMEOUT_CLI_MS:-30000}"

CARIMBO=$(date +%Y%m%d-%H%M%S)
SAIDA="logs/falha-consumidor-${CARIMBO}.log"
mkdir -p logs

registrar() {
    echo "$@" | tee -a "$SAIDA"
}

# Mostra qual consumidor está lendo qual partição, e o lag de cada uma
descrever_grupo() {
    kubectl exec -n "$NAMESPACE" "$BROKER" -- \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --describe --group "$GRUPO" \
        --timeout "$TIMEOUT_CLI" 2>&1 | tee -a "$SAIDA"
}

registrar "=========================================================="
registrar " TESTE DE FALHA DE CONSUMIDOR - $(date)"
registrar "=========================================================="

registrar ""
registrar "--- 1. PREPARANDO O CENARIO: $REPLICAS_TESTE consumidores ---"
registrar "O HPA pode ter reduzido as replicas; forcamos o numero"
registrar "necessario para que haja redistribuicao entre pares."
kubectl scale deployment consumidor --replicas=$REPLICAS_TESTE \
    -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"
kubectl rollout status deployment/consumidor -n "$NAMESPACE" --timeout=120s \
    2>&1 | tee -a "$SAIDA"

registrar ""
registrar "Aguardando o rebalanceamento inicial estabilizar (25s)..."
sleep 25

registrar ""
registrar "--- 2. ESTADO INICIAL ---"
registrar "Esperado: 3 CONSUMER-ID distintos, um por particao."
registrar ""
descrever_grupo

registrar ""
kubectl get pods -l app=consumidor -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"

# Escolhe o primeiro consumidor da lista como vítima
VITIMA=$(kubectl get pods -n "$NAMESPACE" -l app=consumidor \
         -o jsonpath='{.items[0].metadata.name}')

registrar ""
registrar "--- 3. DERRUBANDO O CONSUMIDOR $VITIMA ---"

# A vitima emite REVOGADAS enquanto trata o SIGTERM, ou seja, depois do delete.
LOG_VITIMA=$(mktemp)
timeout 120 kubectl logs -f "$VITIMA" -n "$NAMESPACE" --tail=-1 > "$LOG_VITIMA" 2>/dev/null &
SEGUIDOR=$!
sleep 2

kubectl delete pod "$VITIMA" -n "$NAMESPACE" 2>&1 | tee -a "$SAIDA"
wait "$SEGUIDOR" 2>/dev/null || true
registrar "(capturados $(grep -c REBALANCO "$LOG_VITIMA" || true) eventos de rebalanceamento da vitima antes de ela sair)"

registrar ""
registrar "Aguardando o coordenador detectar e redistribuir (20s)..."
sleep 20

registrar ""
registrar "--- 4. ESTADO APOS A FALHA ---"
registrar "Esperado: as particoes do consumidor removido aparecem sob"
registrar "os CONSUMER-ID restantes. Nenhuma particao fica orfa."
registrar ""
descrever_grupo

registrar ""
registrar "--- 5. AGUARDANDO O KUBERNETES RECRIAR O POD ---"
kubectl rollout status deployment/consumidor -n "$NAMESPACE" --timeout=120s \
    2>&1 | tee -a "$SAIDA"
sleep 25

registrar ""
registrar "--- 6. ESTADO FINAL ---"
registrar "Esperado: novamente 3 consumidores, um por particao."
registrar ""
descrever_grupo

registrar ""
registrar "--- 7. EVENTOS DE REBALANCEAMENTO NOS LOGS ---"
registrar "ATRIBUIDAS = recebeu particoes"
registrar "REVOGADAS  = devolveu de forma ordenada (saida planejada)"
registrar "PERDIDAS   = perdeu sem aviso (sessao expirou)"
registrar "Cada linha mostra o que mudou e, depois da seta, a atribuicao"
registrar "completa daquele consumidor logo apos o evento."
registrar ""
# Junta o que a vitima registrou antes de morrer com o que os sobreviventes
# registraram, em ordem cronologica.
{
    sed "s|^|[pod/$VITIMA] |" "$LOG_VITIMA"
    kubectl logs -l app=consumidor -n "$NAMESPACE" --tail=-1 --prefix 2>/dev/null
} | grep REBALANCO | sort -k2,3 | tee -a "$SAIDA" || \
    registrar "(nenhum evento encontrado)"
rm -f "$LOG_VITIMA"

registrar ""
registrar "Evidencias salvas em: $SAIDA"
