# INF1304-TRABALHO1

rascunho 

# sobe tudo (brokers + 6 sensores)
docker compose up -d

# sobe tudo reconstruindo as imagens (usar sempre que mexer no .py ou no Dockerfile!!!!!!!)
docker compose up -d --build

# sobe só os brokers
docker compose up -d kafka1 kafka2 kafka3

# para tudo, mas preserva os dados
docker compose down

# para tudo e APAGA os volumes (cluster do zero, perde o tópico!!!!!)
docker compose down -v

# ver o que está rodando
docker compose ps

# ver também os que morreram
docker compose ps -a

# acompanhar um serviço
docker compose logs -f sensor_trn

# só o que acontecer de agora em diante (sem o histórico todo)
docker compose logs -f --tail=0 sensor_trn

# um broker
docker logs kafka1

# últimas 50 linhas de tudo
docker compose logs --tail=50

# listar tópicos
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --list

# ver detalhes (partições, líderes, réplicas, ISR)
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic dados-sensores

# quantas mensagens tem em cada partição
docker exec kafka1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka1:19092 --topic dados-sensores

# criar
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --create --topic dados-sensores --partitions 3 --replication-factor 3 --config min.insync.replicas=2

# apagar (esperar uns 10s antes de recriar, a exclusão é assíncrona)
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --delete --topic dados-sensores

# ler do começo, mostrando chave e partição
docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:19092 --topic dados-sensores --from-beginning --property print.key=true --property print.partition=true

# ler só de uma partição específica
docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:19092 --topic dados-sensores --partition 1 --from-beginning
