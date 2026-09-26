# Balanceamento de Carga, Elasticidade e Failover com Kafka

**INF1304 — Distribuição e Concorrência — Primeiro Trabalho**

Sistema de monitoramento de sensores de uma fábrica inteligente construído sobre um
cluster Kafka de 3 brokers, com produtores e consumidores em containers orquestrados
por Kubernetes. O sistema demonstra balanceamento de carga entre consumidores do mesmo
grupo, elasticidade automática via HPA e tolerância a falhas tanto de broker quanto de
consumidor.

**Autores:** Maria Luiza Dutra e Gustavo Rocha Barros

---

## Sumário

1. [O que foi implementado](#1-o-que-foi-implementado)
2. [Arquitetura](#2-arquitetura)
3. [Pré-requisitos](#3-pré-requisitos)
4. [Instalação](#4-instalação)
5. [Operação](#5-operação)
6. [Configuração](#6-configuração)
7. [Testes de falha](#7-testes-de-falha)
8. [Elasticidade](#8-elasticidade)
9. [Balanceamento de carga](#9-balanceamento-de-carga)
10. [Persistência dos dados processados](#10-persistência-dos-dados-processados)
11. [Exibição dos resultados](#11-exibição-dos-resultados)
12. [O que funcionou e o que não funcionou](#12-o-que-funcionou-e-o-que-não-funcionou)
13. [Execução alternativa com Docker Compose](#13-execução-alternativa-com-docker-compose)
14. [Estrutura do repositório](#14-estrutura-do-repositório)
15. [Solução de problemas](#15-solução-de-problemas)

---

## 1. O que foi implementado

O sistema simula uma fábrica com seis máquinas, distribuídas em três setores, que enviam
leituras de sensores continuamente para um cluster Kafka. Um grupo de processadores consome
essas leituras, detecta valores fora dos limites de segurança e grava tudo para análise
posterior. Em linhas gerais, implementamos:

- **Cluster Kafka com 3 brokers** em modo KRaft (sem ZooKeeper), cada um em seu próprio
  StatefulSet no Kubernetes, com volume persistente.
- **Tópico `dados-sensores`** com 3 partições, fator de replicação 3 e
  `min.insync.replicas=2`, criado por um Job a partir da configuração.
- **6 sensores**, cada um um Deployment independente com identidade e setor fixos,
  publicando temperatura, vibração, umidade e consumo de energia a cada 2 segundos.
- **Consumidores em Python** que pertencem ao mesmo grupo e dividem as partições entre si,
  detectando alertas quando algum parâmetro ultrapassa seu limite.
- **Elasticidade automática** com um HorizontalPodAutoscaler que varia os consumidores de
  1 a 3 réplicas conforme o uso de CPU.
- **Persistência** das leituras e dos alertas em arquivos JSON Lines num volume
  compartilhado por todas as réplicas, com escrita concorrente segura sem uso de trava.
- **Scripts de simulação de falha** que derrubam um broker ou um consumidor e registram em
  arquivo o comportamento do sistema antes, durante e depois da falha.
- **Logs de rebalanceamento** marcados com `REBALANCO`, que permitem acompanhar qual
  consumidor recebeu, devolveu ou perdeu cada partição.
- **Um dashboard HTML** gerado a partir dos dados gravados, sem dependências externas.
- **Um Makefile** que concentra todas as operações: construir, subir, observar, gerar carga,
  simular falhas e coletar resultados.

Toda a configuração (endereços, limites, tempos, partições) fica num único arquivo `.env`
e chega aos containers por variáveis de ambiente; não há constantes fixas no código.

---

## 2. Arquitetura

### 2.1 Visão geral

```
                      NAMESPACE fabrinteligente
 ┌──────────────────────────────────────────────────────────────────────┐
 │                                                                      │
 │  PRODUTORES (6 Deployments, 1 réplica cada)                          │
 │  ┌────────────┐ ┌────────────┐ ┌────────────┐                        │
 │  │producao-TRN│ │producao-FRS│ │refriger-CND│  ... (6 máquinas)       │
 │  └──────┬─────┘ └──────┬─────┘ └──────┬─────┘                        │
 │         │              │              │                              │
 │         │  key = SENSOR_ID (partição estável por máquina)             │
 │         └──────────────┴──────────────┘                              │
 │                        │                                             │
 │                        ▼                                             │
 │  CLUSTER KAFKA — modo KRaft, 3 StatefulSets, Service headless        │
 │  ┌─────────────────────────────────────────────────────────────┐     │
 │  │ kafka-1-0        kafka-2-0        kafka-3-0                 │     │
 │  │ broker+controller                                           │     │
 │  │                                                             │     │
 │  │ tópico dados-sensores: 3 partições, RF=3, min.insync=2       │     │
 │  │   P0 [líder+2 réplicas]  P1 [...]  P2 [...]                 │     │
 │  └─────────────────────────────┬───────────────────────────────┘     │
 │                                │  grupo "processadores"              │
 │                                ▼                                     │
 │  CONSUMIDORES (1 Deployment, 1..3 réplicas — HPA)                    │
 │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                  │
 │  │ consumidor-A │ │ consumidor-B │ │ consumidor-C │◄── HorizontalPod │
 │  │     (P0)     │ │     (P1)     │ │     (P2)     │    Autoscaler    │
 │  └───────┬──────┘ └───────┬──────┘ └───────┬──────┘    CPU 8%        │
 │          └────────────────┼────────────────┘                         │
 │                           ▼                                          │
 │  PersistentVolumeClaim "dados-processados" (montado por todas)       │
 │     /dados/dados-processados.jsonl                                   │
 │     /dados/alertas.jsonl                                             │
 └──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  make dados / make dashboard
                    ./dados/*.jsonl  →  ./dados/dashboard.html
```

### 2.2 Produtores (sensores)

Cada sensor é um Deployment separado porque representa uma **máquina física específica**:
tem `SENSOR_ID` e `SETOR` fixos, e não faz sentido escalá-lo horizontalmente. São seis
máquinas distribuídas em três setores:

| Setor | Máquinas |
|---|---|
| `producao` | `producao-TRN`, `producao-FRS` |
| `refrigeracao` | `refrigeracao-CND`, `refrigeracao-CMP` |
| `empacotamento` | `empacotamento-SEL`, `empacotamento-ROT` |

A cada `INTERVALO_SEGUNDOS` o sensor sorteia temperatura, vibração, umidade e consumo de
energia dentro das faixas configuradas e publica um JSON no tópico. A mensagem é enviada
**com chave igual ao `SENSOR_ID`**, o que é uma decisão de projeto importante: o particionador
do Kafka aplica um hash sobre a chave, de modo que todas as leituras de uma mesma máquina
caem sempre na mesma partição, preservando a ordem temporal por máquina. Com 6 sensores e
3 partições, a distribuição observada foi exatamente 2 sensores por partição.

A entrega é assíncrona, confirmada pelo `callback_entrega`, que registra tópico, partição e
offset de cada mensagem aceita pelo broker. O produtor trata dois erros sem morrer:
`KafkaException` (tópico ainda não criado ou cluster indisponível) e `BufferError` (fila
interna cheia, o que acontece quando os brokers ficam fora do ar) — em ambos os casos ele
continua medindo e tenta de novo.

### 2.3 Cluster Kafka

Três brokers em **modo KRaft**, cada um acumulando os papéis `broker` e `controller`, o que
dispensa o ZooKeeper. O quórum de controllers é formado pelos três nós, e portanto tolera a
perda de um deles (quórum = 2 de 3).

Cada broker é um StatefulSet próprio (e não um único StatefulSet com 3 réplicas) porque cada
nó precisa de um `KAFKA_NODE_ID` e de um `KAFKA_ADVERTISED_LISTENERS` distintos, que são
variáveis de ambiente fixas por pod. O Service `kafka-headless` (`clusterIP: None`) não faz
balanceamento: ele apenas dá a cada pod um nome DNS estável (`kafka-1-0.kafka-headless`),
que é o que os clientes Kafka precisam para falar diretamente com o líder de cada partição.

O tópico é criado por um **Job** (`criar-topico`), e não por criação automática — a criação
automática está explicitamente desligada (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) para
garantir que o tópico nasça com o número de partições e o fator de replicação corretos.
As três configurações do tópico vêm do ConfigMap:

| Configuração | Valor | Por quê |
|---|---|---|
| `PARTICOES` | 3 | Define o paralelismo máximo do grupo de consumidores |
| `REPLICACAO` | 3 | Uma réplica em cada broker: tolera a perda de qualquer um |
| `MIN_ISR` | 2 | Com RF=3 e min.insync=2, a escrita continua sendo aceita com 1 broker fora; se 2 caírem, o Kafka rejeita escritas em vez de arriscar perder dados |

### 2.4 Consumidores

Ao contrário dos sensores, os consumidores são **réplicas intercambiáveis do mesmo
Deployment**: todos declaram `group.id = processadores`, e é o coordenador do grupo quem
decide qual réplica lê qual partição. Escalar o Deployment é o que produz o balanceamento.

Decisões de configuração do cliente Kafka (todas em [consumidor/processador.py](consumidor/processador.py)):

- **`partition.assignment.strategy = cooperative-sticky`** — no rebalanço cooperativo apenas
  as partições que realmente mudam de dono são revogadas; as demais continuam sendo
  consumidas durante o processo. O rebalanço ocorre em duas rodadas: na primeira todos
  recebem uma lista vazia de partições novas (as que vão mudar de dono ainda estão com o
  dono antigo); na segunda, depois da revogação, elas chegam ao novo dono. Isso explica as
  linhas com lista vazia nos logs de rebalanço.
- **`enable.auto.commit = true` + `enable.auto.offset.store = false`** — o offset só é
  *marcado* como processado (`store_offsets`) depois que a mensagem foi gravada em disco; o
  commit em si acontece em segundo plano a cada `AUTO_COMMIT_INTERVAL_MS`. Commitar a cada
  mensagem foi testado e descartado: inundava o coordenador, atrasava os heartbeats e
  provocava rebalanceamentos espúrios.
- **`session.timeout.ms = 10000`, `heartbeat.interval.ms = 3000`** — é o *session timeout*
  que define a velocidade do failover: é o tempo que o coordenador espera sem heartbeat
  antes de declarar o consumidor morto e redistribuir suas partições.

O consumidor registra três callbacks de rebalanço, e todos escrevem no log com o marcador
`REBALANCO`, o que torna o processo rastreável com um simples `grep`:

| Callback | Marcador no log | Significado |
|---|---|---|
| `on_assign` | `ATRIBUÍDAS` | Recebeu partições nesta rodada |
| `on_revoke` | `REVOGADAS` | Devolveu partições de forma ordenada (saída planejada). Faz commit síncrono antes de soltar |
| `on_lost` | `PERDIDAS` | Perdeu partições sem aviso (a sessão expirou e o coordenador já as reatribuiu) |

O processamento em si compara cada grandeza contra o limite configurado
(`TEMP_LIMITE`, `VIBRACAO_LIMITE`, `UMIDADE_LIMITE`, `ENERGIA_LIMITE`) e gera um alerta por
parâmetro violado.

### 2.5 Semântica de entrega

A gravação em disco acontece **antes** de o offset ser marcado como processado. Se a
gravação falhar, a exceção sobe, o offset não avança e a mensagem será reprocessada. Isso é
*at-least-once*: nenhuma mensagem é perdida, mas uma mensagem pode ser gravada duas vezes se
um rebalanço ocorrer entre a gravação e o commit. Essas duplicatas são identificáveis pelo
par `(particao, offset)`, que é único no tópico e é gravado em todo registro —
`make dados-resumo` as conta explicitamente.

---

## 3. Pré-requisitos

| Ferramenta | Versão testada | Para quê |
|---|---|---|
| Docker | 24+ | Construir as imagens dos produtores e consumidores |
| Cluster Kubernetes | Docker Desktop / Rancher Desktop (k3s) | Orquestração (alvo principal) |
| `kubectl` | 1.29+ | Aplicar manifestos e inspecionar o cluster |
| `make` | GNU Make 4+ | Todos os comandos de operação |
| `envsubst` | pacote `gettext-base` | Substituir as variáveis do `.env` nos manifestos |
| `python3` | 3.10+ | `make dashboard` e `make validar-dados` (só biblioteca padrão) |
| Conta no Docker Hub | — | Publicar as imagens para o cluster baixar |

Instalação das dependências de sistema no Debian/Ubuntu (ou WSL):

```bash
sudo apt-get update && sudo apt-get install -y make gettext-base python3
```

Recursos mínimos: o cluster consome cerca de **2,5 GB de RAM** e 2 vCPUs, quase tudo por
conta das três JVMs do Kafka (cada broker está limitado a `-Xmx512M`, 768Mi de memória e
500m de CPU). No Docker Desktop, ajuste em *Settings → Resources*.

---

## 4. Instalação

### Passo 1 — Clonar e configurar

```bash
git clone <url-do-repositorio>
cd INF1304-TRABALHO1
cp .env.example .env
```

Abra o `.env` e troque **`DOCKER_USER`** pelo **seu** usuário do Docker Hub. O usuário
`maludu`, que aparece nos exemplos e nas imagens que publicamos, é o nosso; quem for rodar o
projeto precisa usar o próprio, porque o `make push` só consegue publicar no repositório de
quem está logado com `docker login`. Todas as demais
variáveis já vêm com valores funcionais. O `.env` é a única fonte de configuração do
sistema (ver a [seção 6](#6-configuração)).

```bash
DOCKER_USER=seu-usuario-dockerhub
TAG_SENSOR=v1
TAG_CONSUMIDOR=v1
```

### Passo 2 — Construir e publicar as imagens

```bash
docker login
make publicar        # equivale a: make build && make push
```

> **Por que publicar em vez de usar a imagem local?**
> Os manifestos usam `imagePullPolicy: IfNotPresent`. No Docker Desktop, o Kubernetes
> compartilha o mesmo daemon Docker e a imagem local é encontrada — nesse caso basta
> `make build`. Em clusters com containerd próprio (k3s, Rancher Desktop, minikube), a
> imagem construída pelo Docker **não** está visível para o cluster, e é preciso publicá-la
> no registry (`make publicar`) ou importá-la manualmente:
> ```bash
> docker save <seu-usuario>/fabrica-consumidor:v1 | k3s ctr images import -
> docker save <seu-usuario>/fabrica-sensor:v1     | k3s ctr images import -
> ```

### Passo 3 — Gerar o ConfigMap a partir do `.env`

```bash
make configmap
```

Este alvo filtra as variáveis que só interessam ao host (credenciais do Docker Hub, tags de
imagem) e converte o restante do `.env` em [k8s/configmap.yaml](k8s/configmap.yaml). O
arquivo já está versionado com valores válidos, então este passo só é obrigatório se você
alterar o `.env`.

O alvo também cria o namespace, se ainda não existir, e aplica o ConfigMap no cluster. Se o
sistema já estiver no ar, os sensores e consumidores são reiniciados para lerem os valores
novos; numa instalação do zero não há o que reiniciar, e o `make up` do passo seguinte já
sobe tudo com a configuração atualizada.

### Passo 4 — Subir o sistema

```bash
make up
```

O alvo `up` executa, em ordem:

1. `make render` — substitui `${DOCKER_USER}`, `${TAG_*}`, `${PARTICOES}`, `${REPLICACAO}`,
   `${MIN_ISR}`, `${MAX_CONSUMIDORES}` e `${KAFKA_*}` nos manifestos, gravando o resultado em
   `k8s/.rendered/` (diretório ignorado pelo git). Apenas essas variáveis são substituídas:
   as demais precisam chegar intactas aos containers.
2. `make metrics-server` — instala o metrics-server, necessário para o HPA. No Docker
   Desktop ele também aplica o patch `--kubelet-insecure-tls`, sem o qual o metrics-server
   não consegue coletar métricas por causa do certificado autoassinado do kubelet.
3. Aplica namespace → ConfigMap → PVC → brokers.
4. **Espera os 3 brokers ficarem prontos** (até 180s) antes de prosseguir.
5. Executa o Job `criar-topico` e espera sua conclusão.
6. Aplica sensores, consumidor e HPA.

### Passo 5 — Verificar a instalação

```bash
make status     # pods, HPA e uso de CPU/memória
make topico     # partições, líderes, réplicas e ISR
make grupo      # qual consumidor lê qual partição, e o lag
```

Resultado esperado de `make topico`: `PartitionCount: 3`, `ReplicationFactor: 3` e `Isr`
com os três brokers em todas as partições.

### Desinstalação

```bash
make down       # remove o namespace inteiro (e, com ele, todos os recursos e volumes)
```

---

## 5. Operação

Todos os comandos são alvos do [Makefile](Makefile). `make` ou `make help` lista os alvos
disponíveis com sua descrição.

### Ciclo de vida

| Comando | O que faz |
|---|---|
| `make build` | Constrói as imagens do sensor e do consumidor |
| `make push` | Publica as imagens no Docker Hub |
| `make publicar` | `build` + `push` |
| `make render` | Renderiza os manifestos em `k8s/.rendered/` |
| `make metrics-server` | Instala/corrige o metrics-server (pré-requisito do HPA) |
| `make up` | Sobe todo o sistema |
| `make down` | Remove todos os recursos |
| `make configmap` | Regenera o ConfigMap a partir do `.env` e reinicia os pods |

### Observação do sistema

| Comando | O que faz |
|---|---|
| `make status` | Pods, HPA e uso de recursos |
| `make topico` | Partições, líderes, réplicas e ISR do tópico |
| `make offsets` | Quantas mensagens há em cada partição |
| `make grupo` | **Qual consumidor lê qual partição, e o lag de cada uma** |
| `make logs-sensores` | Acompanha os logs dos 6 sensores |
| `make logs-consumidor` | Acompanha os logs de todos os consumidores |
| `make rebalanceamento` | Filtra apenas os eventos `REBALANCO` dos consumidores |
| `make watch-hpa` | Acompanha o HPA escalando em tempo real |

### Dados e resultados

| Comando | O que faz |
|---|---|
| `make dados` | Copia os arquivos JSON Lines do volume para `./dados/` |
| `make alertas` | Mostra os últimos 20 alertas gravados, sem copiar nada |
| `make dados-resumo` | Registros por consumidor, por partição, alertas por parâmetro e duplicatas |
| `make validar-dados` | Verifica que toda linha é um JSON válido (prova do append atômico) |
| `make dashboard` | Gera `dados/dashboard.html` |
| `make limpar-dados` | Esvazia os arquivos no volume para começar um teste limpo |
| `make resetar-offsets` | Pula as mensagens acumuladas no tópico, levando o grupo para o fim de cada partição |
| `make recriar-topico` | Apaga e recria o tópico, zerando os offsets de verdade |

### Carga e escala

| Comando | O que faz |
|---|---|
| `make carga-alta` | Reduz o intervalo dos sensores para 0,02 s (~300 msg/s) e força o HPA a escalar |
| `make carga-normal` | Volta ao intervalo configurado no `.env` |
| `make escalar N=3` | Define manualmente o número de consumidores |
| `make escalar-sensores N=10` | Aumenta a carga com mais réplicas de todas as máquinas, em vez de acelerá-las |
| `make escalar-maquina MAQUINA=trn N=10` | Réplicas de uma máquina só (ver a [seção 8](#8-elasticidade)) |

### Testes de falha

| Comando | O que faz |
|---|---|
| `make falha-broker` | Derruba um broker e grava as evidências em `logs/` |
| `make falha-consumidor` | Derruba um consumidor e grava o rebalanço em `logs/` |

### Roteiro sugerido para a demonstração

```bash
make up                       # 1. sobe o sistema
make topico                   # 2. mostra 3 partições, RF=3, ISR completo
make escalar N=3              # 3. três consumidores no grupo
make grupo                    # 4. uma partição para cada: balanceamento de carga
make falha-consumidor         # 5. failover + rebalanço automático
make falha-broker             # 6. tolerância a falha do cluster Kafka
make carga-alta               # 7. aumenta a carga...
make watch-hpa                #    ...e o HPA escala os consumidores
make carga-normal
make dados-resumo             # 8. distribuição por consumidor e por partição
make validar-dados            # 9. integridade dos arquivos concorrentes
make dashboard                # 10. abre dados/dashboard.html
```

---

## 6. Configuração

Não há valores fixos no código. Toda a configuração parte de um único arquivo, `.env`, e
chega aos containers por variáveis de ambiente:

```
.env  ──(make configmap)──►  k8s/configmap.yaml  ──(envFrom)──►  containers
  │
  └────(make render / envsubst)────►  k8s/.rendered/*.yaml
```

- **`.env`** é a fonte única. Está no `.gitignore`; o modelo versionado é
  [.env.example](.env.example).
- **ConfigMap** (`envFrom: configMapRef`) entrega as variáveis de aplicação aos pods dos
  sensores, dos consumidores e do Job de criação do tópico. Sensores e consumidores leem
  exatamente as mesmas variáveis — é por isso que o limite desenhado no gráfico do dashboard
  é garantidamente o mesmo que gerou o alerta.
- **`envsubst`** substitui apenas as variáveis que precisam aparecer na *estrutura* do
  manifesto (nome da imagem, `maxReplicas` do HPA, número de partições), listadas em
  `SUBST_VARS`. As demais não são substituídas de propósito: precisam chegar intactas para
  serem resolvidas dentro do container.

| Grupo | Variáveis |
|---|---|
| Kafka | `KAFKA_CLUSTER_ID`, `KAFKA_IMAGEM`, `KAFKA_BOOTSTRAP`, `TOPICO_SENSORES`, `PARTICOES`, `REPLICACAO`, `MIN_ISR`, `GRUPO_CONSUMIDORES` |
| Protocolo de grupo | `SESSION_TIMEOUT_MS`, `HEARTBEAT_INTERVAL_MS`, `MAX_POLL_INTERVAL_MS`, `AUTO_COMMIT_INTERVAL_MS` |
| Sensores | `INTERVALO_SEGUNDOS`, `TEMP_MIN/MAX`, `VIBRACAO_MIN/MAX`, `UMIDADE_MIN/MAX`, `ENERGIA_MIN/MAX` |
| Limites de alerta | `TEMP_LIMITE`, `VIBRACAO_LIMITE`, `UMIDADE_LIMITE`, `ENERGIA_LIMITE` |
| Persistência | `PERSISTENCIA_ATIVA`, `DIR_DADOS`, `ARQUIVO_DADOS`, `ARQUIVO_ALERTAS` |
| Escala | `MAX_CONSUMIDORES`, `HPA_CPU_ALVO` |
| Ferramentas do host | `DOCKER_USER`, `TAG_SENSOR`, `TAG_CONSUMIDOR`, `TIMEOUT_CLI_MS` |

A identidade de cada pod não vem do `.env`, e sim do próprio Kubernetes: o consumidor recebe
`CONSUMER_ID` via `fieldRef: metadata.name`, o que faz o nome do pod aparecer nos logs de
rebalanço e em cada registro gravado em disco.

### Documentação do código

Todos os módulos, classes e funções têm docstring no estilo Google (`Args:`, `Returns:`,
`Raises:`). A conformidade é verificada pelo
[ruff](ruff.toml), que tem as regras `D` (pydocstyle) e `DOC` (pydoclint) ativadas:

```bash
pip install -r requirements-dev.txt
ruff check .
```

---

## 7. Testes de falha

### 7.1 Falha de broker — `make falha-broker`

**O que o teste demonstra:** com `RF=3` e `min.insync.replicas=2`, o cluster continua
operando normalmente com apenas 2 brokers, elegendo novos líderes automaticamente; e o
StatefulSet do Kubernetes recria o pod derrubado sem intervenção manual.

**O que o script faz:** registra o estado do tópico (`--describe`), derruba `kafka-2-0` com
`kubectl delete pod`, registra o estado durante a falha, espera o Kubernetes recriar o pod,
aguarda 30 s de ressincronização e registra o estado final. Todas as consultas são feitas
a partir de `kafka-1-0`, um broker que permanece no ar — o fato de a consulta responder já é
parte da evidência.

**Resultado obtido** ([logs/falha-broker-20260925-204040.log](logs/falha-broker-20260925-204040.log)):
com `kafka-2-0` fora, o broker 1 assumiu a liderança da partição 1 e o ISR das três partições
caiu para `1,3`. O cluster continuou respondendo, as escritas seguiram sendo aceitas e
sensores e consumidor não reiniciaram. O StatefulSet recriou o pod em poucos segundos e,
depois da ressincronização, o ISR voltou a ter os três brokers.

Vale notar que o broker 2 voltou ao ISR mas não retomou a liderança da partição 1. Esse é o
comportamento padrão do Kafka: reequilibrar os líderes exige eleição explícita
(`kafka-leader-election.sh`) ou a opção `auto.leader.rebalance.enable`.

### 7.2 Falha de consumidor — `make falha-consumidor`

**O que o teste demonstra:** as partições do consumidor que caiu são redistribuídas
automaticamente entre os que restaram, sem intervenção manual e sem nenhuma partição órfã; e
o pod recriado pelo Deployment reingressa no grupo disparando um novo rebalanço.

**O que o script faz:** força 3 réplicas (o HPA pode ter reduzido), espera o rebalanço
inicial estabilizar, registra a atribuição inicial, derruba a primeira réplica da lista e
registra a atribuição depois da falha e depois da recuperação. O script também segue os logs
da vítima em segundo plano **antes** do `delete`, porque ela emite `REVOGADAS` enquanto trata
o `SIGTERM` — ou seja, depois do comando de remoção. No fim, junta cronologicamente os
eventos `REBALANCO` da vítima e dos sobreviventes.

**Resultado obtido** ([logs/falha-consumidor-20260925-204210.log](logs/falha-consumidor-20260925-204210.log)):
com 3 réplicas, cada uma ficou com uma partição. Ao derrubar `fw2f8`, dono da partição 1, a
vítima registrou `REVOGADAS [1]` ao tratar o `SIGTERM`, e o pod recriado pelo Deployment
(`l8kxp`) recebeu a partição 1 cerca de um segundo depois. Nenhuma partição ficou sem dono e
o lag não passou de 6 mensagens durante todo o teste.

Os eventos `REBALANCO` de todas as réplicas, incluindo os da vítima, aparecem em ordem
cronológica no fim do arquivo.

---

## 8. Elasticidade

O Deployment `consumidor` é governado por um HorizontalPodAutoscaler
([k8s/hpa.yaml](k8s/hpa.yaml)):

| Parâmetro | Valor | Justificativa |
|---|---|---|
| `minReplicas` | 1 | Uma réplica basta com a carga padrão de 3 msg/s |
| `maxReplicas` | `${MAX_CONSUMIDORES}` = 3 | **Teto deliberado, não arbitrário:** o tópico tem 3 partições e, no Kafka, cada partição é atribuída a no máximo um consumidor do grupo. Uma 4ª réplica ficaria permanentemente ociosa |
| Métrica | CPU, `averageUtilization: ${HPA_CPU_ALVO}` = 8 % | Calculada sobre `requests.cpu = 100m`, ou seja, o alvo é de 8m de CPU por réplica. O valor foi medido neste cluster: com os 6 sensores o consumidor usa de 3m a 4m, e com 60 sensores sobe para cerca de 18m. Um alvo de 8 % fica entre os dois, então o HPA não oscila com a carga normal e escala assim que o número de produtores aumenta |
| `scaleDown.stabilizationWindowSeconds` | 60 | Espera 60 s de calmaria antes de reduzir, evitando subir e descer repetidamente em oscilações curtas |

### Como demonstrar

```bash
make watch-hpa     # em um terminal, acompanha HPA e pods
make carga-alta    # em outro: intervalo 0,02 s (~300 msg/s, 100x a carga padrão)
# ... o HPA escala de 1 para 3 réplicas ...
make grupo         # confirma uma partição por réplica
make carga-normal  # volta ao intervalo padrão; após 60s de calmaria, o HPA reduz
```

`make carga-alta` não reconstrói imagem nenhuma: ele faz `kubectl patch` no ConfigMap
alterando `INTERVALO_SEGUNDOS` e reinicia os sensores — mais uma demonstração de que a
configuração é externa ao código.

**Aumentando o número de sensores em vez da taxa.** Outra forma de gerar carga, sem mexer
em nenhuma configuração, é multiplicar os produtores com `make escalar-sensores N=10`, que
coloca 10 réplicas de cada uma das 6 máquinas. Como as chaves continuam sendo os mesmos 6
`SENSOR_ID`, a carga extra se espalha igualmente pelas 3 partições e o HPA consegue dividi-la
entre os consumidores.

```bash
make watch-hpa                 # em um terminal, acompanha HPA e pods
make escalar-sensores N=10     # em outro: 60 sensores no total
make grupo                     # depois que o HPA escalar, uma partição por consumidor
make escalar-sensores N=1      # volta a 6 sensores; após 60s de calmaria, o HPA reduz
```

No teste registrado em
[logs/hpa-escala-sensores-20260925-224416.log](logs/hpa-escala-sensores-20260925-224416.log),
a CPU do consumidor foi de 4 % para 22 % cerca de um minuto depois da subida para 60 sensores,
e o HPA passou de 1 para 3 réplicas, cada uma com uma partição e lag abaixo de 20 mensagens.
Com a carga dividida, a média estabilizou em torno de 8 %. Ao voltar para 6 sensores, a CPU
caiu para 2 % e o HPA reduziu para 1 réplica depois da janela de estabilização. Os pods de
sensor removidos ficam cerca de 30 s em `Terminating`, porque o Kubernetes espera esse prazo
antes de encerrá-los à força.

Já escalar **uma única máquina** (`make escalar-maquina MAQUINA=trn N=10`) mostra um limite
do particionamento por chave: todas as réplicas enviam com o mesmo `SENSOR_ID`, caem na
mesma partição e sobrecarregam um único consumidor. O HPA pode até criar novas réplicas,
mas elas recebem partições que não estão sobrecarregadas, e a partição quente continua com um
consumidor só. Distribuir essa carga exigiria chaves diferentes por réplica, ao custo de
perder a ordem das leituras daquela máquina.

**Evidência** ([logs/hpa-20260925-203921.log](logs/hpa-20260925-203921.log) e
[logs/rebalanceamento-20260925-204436.log](logs/rebalanceamento-20260925-204436.log)): o
acompanhamento do HPA mostra o Deployment passando de 1 para 3 réplicas durante o teste de
falha de consumidor e, com a CPU em 2 %, bem abaixo do alvo, o HPA reduzindo sozinho de
volta para 1 réplica às 20:44. O log de rebalanceamento mostra o consumidor que sobrou
reassumindo as partições 0 e 1 logo em seguida.

---

## 9. Balanceamento de carga

O balanceamento acontece em duas camadas independentes:

**Do lado do produtor**, a chave da mensagem (`SENSOR_ID`) determina a partição por hash.
Isso distribui as 6 máquinas entre as 3 partições e, ao mesmo tempo, garante que as leituras
de uma mesma máquina nunca saiam de ordem. A distribuição obtida é perfeitamente uniforme:

```
$ make dados-resumo
== Registros por partição ==
    384 0
    384 1
    384 2

== Registros por sensor ==
    192 empacotamento-ROT     192 producao-FRS        192 refrigeracao-CMP
    192 empacotamento-SEL     192 producao-TRN        192 refrigeracao-CND
```

**Do lado do consumidor**, o coordenador do grupo `processadores` atribui as partições às
réplicas vivas. Com 3 réplicas, a saída de `make grupo` mostra três `CONSUMER-ID` distintos,
um por partição — cada réplica lê a sua própria partição:

```
GROUP          TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
processadores  dados-sensores  0          1916            1916            0    consumidor-...-pr84r
processadores  dados-sensores  1          1916            1916            0    consumidor-...-fw2f8
processadores  dados-sensores  2          1914            1916            2    consumidor-...-s5cmj
```

---

## 10. Persistência dos dados processados

Os consumidores gravam em dois arquivos JSON Lines (um objeto JSON por linha) dentro de um
volume persistente compartilhado por **todas** as réplicas:

| Arquivo | Conteúdo |
|---|---|
| `/dados/dados-processados.jsonl` | Uma linha por leitura processada |
| `/dados/alertas.jsonl` | Uma linha por parâmetro que violou seu limite |

Exemplo de registro de leitura:

```json
{"gravado_em":"2026-09-25T23:39:06.544+00:00","consumidor":"consumidor-748d5c9d97-s5cmj",
 "particao":2,"offset":1700,"sensor_id":"empacotamento-ROT","setor":"empacotamento",
 "timestamp":1790379546.54,"temperatura":20.68,"vibracao":6.51,"umidade":68.42,
 "consumo_energia":140.56,"alerta":false}
```

Cada registro carrega **quem** gravou (`consumidor`), **de onde veio** (`particao`,
`offset`) e **quando** (`gravado_em`), o que é o que permite auditar o balanceamento e
reconhecer duplicatas depois de um rebalanço.

### O problema de concorrência e como foi resolvido

Várias réplicas escrevem nos **mesmos dois arquivos ao mesmo tempo**. A consistência é obtida
sem trava explícita, apoiando-se em duas propriedades combinadas
([consumidor/persistencia.py](consumidor/persistencia.py)):

1. Os arquivos são abertos em modo *append* (`"ab"`, que usa `O_APPEND`). Isso torna
   "posicionar no fim do arquivo e escrever" uma operação indivisível no kernel: duas
   réplicas nunca gravam sobre a mesma região do arquivo.
2. Cada registro é gravado com uma **única chamada de escrita**, em modo binário sem buffer
   (`buffering=0`). Assim o buffer do Python não parte uma linha em duas chamadas, o que
   permitiria a outra réplica se intercalar no meio de um registro.

O limite portável para essa garantia é `PIPE_BUF` (4096 bytes). Os registros deste sistema
têm cerca de 250 bytes; registros maiores seriam gravados mesmo assim, mas com um aviso no
log. Escritas parciais também são detectadas e registradas.

A prova de que funciona é `make validar-dados`, que tenta desserializar cada linha dos dois
arquivos: se houvesse intercalação, haveria linhas truncadas e o JSON falharia.

```
$ make validar-dados
dados/dados-processados.jsonl: 1152 linhas, 0 corrompidas
dados/alertas.jsonl: 680 linhas, 0 corrompidas
```

### Escolha do volume

O PVC usa `ReadWriteOnce`, que no Kubernetes restringe a montagem a **um nó**, não a um pod.
Como o cluster de desenvolvimento tem um nó só, as três réplicas montam o mesmo volume sem
conflito. Em um cluster com vários nós seria necessário `ReadWriteMany`, que o
provisionador padrão do Docker Desktop não oferece — esta é a principal limitação de
portabilidade do sistema (ver [seção 12](#12-o-que-funcionou-e-o-que-não-funcionou)).

---

## 11. Exibição dos resultados

### 11.1 Dashboard HTML

```bash
make dados        # copia os arquivos do volume para ./dados/
make dashboard    # gera ./dados/dashboard.html
```

[dashboard/gerar_dashboard.py](dashboard/gerar_dashboard.py) lê os arquivos JSON Lines,
agrega os números e escreve **um único arquivo HTML autocontido**: sem CDN, sem servidor e
sem biblioteca de terceiros (os gráficos são SVG gerado à mão). A página abre offline,
funciona em tema claro e escuro e pode ser anexada ao relatório.

O dashboard mostra:

- **KPIs**: total de leituras, alertas, consumidores ativos e partições.
- **Balanceamento**: leituras por consumidor e por partição, e uma matriz consumidor ×
  partição que torna visível o rebalanço (um consumidor que assumiu a partição de outro
  aparece com registros em duas colunas).
- **Linha do tempo** por consumidor, em colunas empilhadas: é onde o momento do rebalanço
  fica evidente, porque a faixa de um consumidor termina e a de outro começa.
- **Séries por sensor** de cada grandeza, com a linha do limite de perigo desenhada.
- **Alertas** por parâmetro e por sensor, com os mais recentes em tabela.
- **Estado ao vivo do cluster** (réplicas, HPA e lag por partição), se o cluster estiver no
  ar no momento da geração; se estiver fora, a página é gerada do mesmo jeito, sem esse
  painel.

Nenhum valor é fixo no código do dashboard: os nomes dos arquivos, os limites de perigo e os
nomes do tópico e do grupo vêm das mesmas variáveis de ambiente que o consumidor usa, de modo
que o limite desenhado no gráfico é exatamente o que gerou o alerta.

### 11.2 Números da coleta

O resumo da coleta está em
[logs/dados-resumo-20260925-204531.log](logs/dados-resumo-20260925-204531.log), e o
dashboard gerado a partir dela em [dados/dashboard.html](dados/dashboard.html). Foram
**1.152 leituras** e **680 alertas**, divididos igualmente entre as partições (384 em cada)
e entre os sensores (192 cada). As leituras foram gravadas por **4 consumidores diferentes**
ao longo da janela, reflexo das réplicas que entraram e saíram durante os testes, sem
nenhuma linha corrompida.

Os logs `estado-inicial-*`, `estado-final-*` e `pods-*` em [logs/](logs/) registram o
estado do cluster antes e depois da sessão de testes.

---

## 12. O que funcionou e o que não funcionou

### Funcionou como esperado

- **Cluster Kafka KRaft com 3 brokers**, sem ZooKeeper, subindo de forma confiável a partir
  do zero com `make up`.
- **Tópico com 3 partições e RF=3**, criado por Job a partir das variáveis de configuração.
- **Balanceamento automático**: 3 consumidores ↔ 3 partições, uma para cada.
- **Failover de broker**: eleição de novo líder em segundos, ISR reduzido a 2, escritas
  continuando, pod recriado pelo StatefulSet, ISR voltando a 3.
- **Failover de consumidor**: reatribuição automática da partição órfã, lag máximo de 6
  mensagens, nenhuma partição sem dono.
- **Elasticidade**: HPA escalando de 1 a 3 réplicas por uso de CPU.
- **Persistência concorrente**: 1.832 linhas escritas por réplicas simultâneas, 0 corrompidas.
- **Configuração 100 % externa**: nenhuma constante hard-coded; trocar o intervalo dos
  sensores ou um limite de alerta é um `kubectl patch` no ConfigMap, sem rebuild.

### Problemas encontrados e resolvidos durante o desenvolvimento

| Problema | Diagnóstico | Solução |
|---|---|---|
| Rebalanceamentos espúrios sob carga | Commit síncrono a cada mensagem inundava o coordenador e atrasava os heartbeats | `enable.auto.offset.store=false` + `store_offsets()` manual, com commit em background a cada 5 s |
| Rebalanço interrompia o consumo de todas as partições | Estratégia de atribuição *eager* (padrão) revoga tudo a cada rodada | Troca para `cooperative-sticky`: só as partições que mudam de dono são revogadas |
| `kafka-consumer-groups.sh --describe` estourando por timeout | O padrão de 5 s é curto demais: a CLI sobe uma JVM dentro do próprio broker, que está limitado a 500m de CPU | Variável `TIMEOUT_CLI_MS=30000`, usada no Makefile e nos scripts |
| HPA com `<unknown>` nas métricas | O metrics-server não coletava por causa do certificado autoassinado do kubelet no Docker Desktop | Alvo `make metrics-server` aplica o patch `--kubelet-insecure-tls` automaticamente |
| Os eventos `REVOGADAS` da vítima não apareciam no log do teste | O pod emite `REVOGADAS` ao tratar o `SIGTERM`, ou seja, **depois** do `kubectl delete` | O script passou a seguir os logs da vítima em segundo plano antes do delete e a juntá-los cronologicamente com os dos sobreviventes |
| Linhas truncadas nos arquivos compartilhados | O buffer do Python partia um registro em duas chamadas de escrita, permitindo intercalação | Abertura em `"ab"` com `buffering=0` e uma única chamada de escrita por registro |

### Limitações conhecidas

- **`ReadWriteOnce` amarra o sistema a um nó.** As réplicas do consumidor só compartilham o
  volume porque o cluster tem um único nó. Em um cluster multi-nó seria preciso
  `ReadWriteMany` (NFS, CephFS ou similar) ou, preferencialmente, trocar os arquivos por um
  banco de dados, que seria a evolução natural da camada de persistência.
- **O broker que volta não retoma a liderança.** É o comportamento padrão do Kafka. Para
  reequilibrar as lideranças seria preciso `auto.leader.rebalance.enable=true` ou uma eleição
  explícita. Foi deixado assim de propósito, para que a evidência do failover continuasse
  visível no estado final do log.
- **Semântica *at-least-once*, não *exactly-once*.** Um rebalanço entre a gravação e o commit
  faz a mesma leitura ser gravada duas vezes. Isso é uma escolha consciente (é o que garante
  que nenhuma mensagem se perca), e as duplicatas são detectáveis pelo par
  `(particao, offset)`. *Exactly-once* exigiria transações Kafka e escrita idempotente.
- **O teto de 3 consumidores é uma limitação do desenho do tópico, não do HPA.** Escalar além
  de 3 exigiria aumentar o número de partições — `PARTICOES` e `MAX_CONSUMIDORES` no `.env`
  devem ser alterados juntos.
- **Sem autenticação nem TLS.** Todos os listeners são `PLAINTEXT`. É adequado para um
  ambiente de desenvolvimento isolado por namespace, mas não para produção.
- **Os testes de falha são scripts de demonstração, não testes automatizados.** Eles
  registram evidências para inspeção humana; não há asserções nem código de saída
  significativo em caso de comportamento inesperado.

---

## 13. Execução alternativa com Docker Compose

O [docker-compose.yml](docker-compose.yml) sobe o mesmo sistema sem Kubernetes. É útil para
desenvolver e testar o código dos produtores e consumidores rapidamente, mas **não** oferece
HPA nem volume compartilhado — a elasticidade e o self-healing só existem na versão
Kubernetes.

```bash
cp .env.example .env
docker compose up -d --build           # 3 brokers + 6 sensores + 1 consumidor

# o tópico NÃO é criado automaticamente nesta versão; crie-o na mão:
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --create --topic dados-sensores \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2

docker compose up -d --scale consumidor=3   # três consumidores no mesmo grupo
docker compose logs -f consumidor           # acompanha o rebalanço
```

Comandos úteis:

```bash
docker compose ps                      # o que está rodando
docker compose logs -f sensor_trn      # acompanha um sensor
docker compose down                    # para tudo, preserva os volumes
docker compose down -v                 # para tudo e APAGA os volumes (perde o tópico)

# detalhes do tópico: partições, líderes, réplicas e ISR
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe --topic dados-sensores

# quantas mensagens há em cada partição
docker exec kafka1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka1:19092 --topic dados-sensores

# simular a falha de um broker
docker stop kafka2
```

Os brokers também expõem portas externas (`29092`, `29093`, `29094`) para clientes rodando
no host. Diferenças em relação à versão Kubernetes: o consumidor grava em `/dados` **dentro
do container** (volume efêmero, perdido no `down`), o tópico precisa ser criado manualmente
e não há HPA.

---

## 14. Estrutura do repositório

```
.
├── Makefile                      # todos os comandos de operação (make help)
├── .env.example                  # modelo de configuração — copie para .env
├── docker-compose.yml            # execução alternativa, sem Kubernetes
├── ruff.toml                     # linter, com as regras de docstring ativadas
│
├── produtor/
│   ├── sensor.py                 # produtor Kafka: gera e publica as leituras
│   ├── Dockerfile
│   └── requirements.txt
│
├── consumidor/
│   ├── processador.py            # consumidor Kafka, callbacks de rebalanço, detecção de alertas
│   ├── persistencia.py           # gravação concorrente em JSON Lines com append atômico
│   ├── Dockerfile
│   └── requirements.txt
│
├── k8s/
│   ├── namespace.yaml            # namespace fabrinteligente
│   ├── configmap.yaml            # configuração gerada a partir do .env
│   ├── kafka.yaml                # Service headless + 3 StatefulSets (brokers KRaft)
│   ├── criar-topico.yaml         # Job que cria o tópico com partições e replicação
│   ├── sensores.yaml             # 6 Deployments, um por máquina
│   ├── consumidor.yaml           # Deployment do processador + montagem do volume
│   ├── hpa.yaml                  # HorizontalPodAutoscaler (1..3 réplicas, CPU 8%)
│   ├── pvc-dados.yaml            # volume compartilhado pelos consumidores
│   └── .rendered/                # gerado por make render (ignorado pelo git)
│
├── scripts/
│   ├── falha-broker.sh           # derruba um broker e registra as evidências
│   └── falha-consumidor.sh       # derruba um consumidor e registra o rebalanço
│
├── dashboard/
│   └── gerar_dashboard.py        # gera dados/dashboard.html a partir dos JSON Lines
│
├── logs/                         # evidências de execução dos testes
│   ├── falha-broker-*.log
│   ├── falha-consumidor-*.log
│   ├── rebalanceamento-*.log
│   ├── hpa-*.log, pods-*.log
│   ├── estado-inicial-*.log, estado-final-*.log
│   └── dados-resumo-*.log
│
└── dados/                        # amostra dos dados processados (make dados)
    ├── dados-processados.jsonl
    ├── alertas.jsonl
    └── dashboard.html
```

---

## 15. Solução de problemas

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ImagePullBackOff` nos pods de sensor ou consumidor | `DOCKER_USER` errado no `.env`, ou as imagens não foram publicadas | `make publicar` e `make render && make up`. Em k3s/minikube, importe a imagem manualmente (ver [passo 2](#passo-2--construir-e-publicar-as-imagens)) |
| Pods do Kafka em `CrashLoopBackOff` na primeira subida | Volume com dados de um cluster anterior (`KAFKA_CLUSTER_ID` diferente) | `make down` e apague os PVCs: `kubectl delete pvc --all -n fabrinteligente` |
| Job `criar-topico` falhando | Brokers ainda não prontos | O Job tem `backoffLimit: 4` e tenta de novo sozinho. Se persistir: `kubectl logs job/criar-topico -n fabrinteligente` |
| HPA mostrando `<unknown>` em TARGETS | metrics-server ausente ou sem o patch de TLS | `make metrics-server` e espere ~1 min pela primeira coleta |
| `make grupo` estourando por timeout | A CLI sobe uma JVM dentro do broker, que tem 500m de CPU | Aumente `TIMEOUT_CLI_MS` no `.env` |
| `make grupo` mostrando `Consumer group has no active members` | Nenhum consumidor rodando, ou rebalanço em andamento | `kubectl get pods -l app=consumidor -n fabrinteligente` e espere alguns segundos |
| Consumidores ociosos com o lag crescendo | Mais réplicas do que partições | Cada partição vai para no máximo um consumidor: `make escalar N=3` no máximo, ou aumente `PARTICOES` |
| `envsubst: command not found` | Pacote `gettext-base` ausente | `sudo apt-get install gettext-base` |
| Lag de milhões de mensagens e grupo sempre em *rebalancing* | Os sensores ficaram em carga alta e o atraso acumulou; consumidores reiniciando por falta de memória | `make carga-normal` e `make escalar-sensores N=1`, depois `make resetar-offsets` |
| Dashboard vazio | `dados/` sem arquivos | `make dados` com o cluster no ar, e então `make dashboard` |
| Pods sendo mortos por `OOMKilled` | Memória insuficiente no Docker Desktop | Aumente para pelo menos 4 GB em *Settings → Resources* |
