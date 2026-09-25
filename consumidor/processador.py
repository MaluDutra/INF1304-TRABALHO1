"""Processador de dados de sensores Kafka."""

import json
import logging
import os
import signal
import socket
from datetime import datetime
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from persistencia import RepositorioJsonl


class ConsumidorSensor:
    """Consome dados de sensores do Kafka.

    Args:
        id_consumidor: Identificador desta instância do consumidor.
        brokers_kafka: Brokers Kafka, separados por vírgula.
        topico_sensor: Tópico de onde os dados dos sensores são lidos.
        grupo_consumidor: Grupo de consumidores usado para dividir as partições.
    """

    def __init__(
        self,
        id_consumidor: str,
        brokers_kafka: str,
        topico_sensor: str,
        grupo_consumidor: str,
    ) -> None:
        """Inicializa o consumidor Kafka de dados de sensores.

        Args:
            id_consumidor: Identificador desta instância do consumidor.
            brokers_kafka: Brokers Kafka, separados por vírgula.
            topico_sensor: Tópico de onde os dados dos sensores são lidos.
            grupo_consumidor: Grupo de consumidores usado para dividir as partições.
        """
        self.id_consumidor = id_consumidor
        self.brokers_kafka = brokers_kafka
        self.topico_sensor = topico_sensor
        self.grupo_consumidor = grupo_consumidor
        self.logger = self._configurar_logger()
        self.rodando = False
        self.repositorio = self._configurar_persistencia()

        # Configuração do consumidor Kafka
        consumidor_config = {
            "bootstrap.servers": self.brokers_kafka,
            "group.id": self.grupo_consumidor,
            "client.id": self.id_consumidor,
            "auto.offset.reset": "earliest",
            # Commit periódico em background, mas apenas dos offsets que o código
            # marcou explicitamente como processados. Evita inundar o coordenador
            # com um commit por mensagem, o que atrasava os heartbeats e acabava
            # provocando rebalanceamentos.
            "enable.auto.commit": True,
            # Desliga o registro automático de offsets
            "enable.auto.offset.store": False,
            "auto.commit.interval.ms": int(os.getenv("AUTO_COMMIT_INTERVAL_MS", "5000")),
            # Minimiza redistribuições: só as partições que mudam de dono são
            # revogadas, as demais seguem sendo consumidas durante o rebalanço.
            "partition.assignment.strategy": "cooperative-sticky",
            # Tempo sem heartbeat até o coordenador considerar o consumidor morto
            # e disparar o rebalanço (define a velocidade do failover)
            "session.timeout.ms": int(os.getenv("SESSION_TIMEOUT_MS", "10000")),
            "heartbeat.interval.ms": int(os.getenv("HEARTBEAT_INTERVAL_MS", "3000")),
            "max.poll.interval.ms": int(os.getenv("MAX_POLL_INTERVAL_MS", "300000")),
        }

        try:
            self.consumidor = Consumer(consumidor_config)
            self.logger.info("Consumidor Kafka inicializado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar o consumidor Kafka: {e}")
            raise

        # Assinatura do tópico de sensores
        self.consumidor.subscribe(
            [self.topico_sensor],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )
        self.logger.info(f"Assinando tópico: {self.topico_sensor}")

        # Configurar sinais de encerramento
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _configurar_logger(self) -> logging.Logger:
        """Configuração do logger."""
        # O nível de log pode ser configurado via variável de ambiente LOG_LEVEL
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        return logging.getLogger(self.id_consumidor)

    def _configurar_persistencia(self) -> RepositorioJsonl | None:
        """Cria o repositório que grava as leituras e os alertas em arquivo.

        O diretório configurado em DIR_DADOS é o ponto de montagem do volume
        compartilhado por todas as réplicas do consumidor.

        Returns:
            Repositório pronto para gravar, ou None se PERSISTENCIA_ATIVA estiver
            desligada (útil para rodar o consumidor sem volume montado).
        """
        if os.getenv("PERSISTENCIA_ATIVA", "true").lower() != "true":
            self.logger.warning("Persistência desativada: os dados não serão gravados em arquivo")
            return None

        return RepositorioJsonl(
            diretorio=os.getenv("DIR_DADOS", "/dados"),
            arquivo_dados=os.getenv("ARQUIVO_DADOS", "dados-processados.jsonl"),
            arquivo_alertas=os.getenv("ARQUIVO_ALERTAS", "alertas.jsonl"),
            id_consumidor=self.id_consumidor,
            logger=self.logger,
        )

    def on_assign(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado quando o coordenador atribui partições a este consumidor.

        Com a estratégia cooperative-sticky, `particoes` traz só as partições NOVAS
        desta rodada, não a atribuição completa. O rebalanço cooperativo ocorre em
        duas rodadas: na primeira, as partições que vão mudar de dono ainda estão
        com o dono antigo, e por isso todos recebem uma lista vazia; na segunda,
        depois que o dono antigo as revogou, elas chegam ao novo dono.

        Apenas registra o evento no log; a atribuição em si é feita pela biblioteca.

        Args:
            consumidor: Instância do consumidor Kafka que recebeu as partições.
            particoes: Partições adicionadas a este consumidor nesta rodada.
        """
        novas = {p.partition for p in particoes}
        # A biblioteca só aplica a atribuição depois que este callback retorna,
        # então assignment() ainda não inclui as partições novas
        atual = {p.partition for p in consumidor.assignment()} | novas

        if not novas:
            # Primeira rodada do rebalanço cooperativo, ou consumidor que não mudou
            self.logger.debug(
                f"REBALANCO - nenhuma partição nova para {self.id_consumidor} nesta rodada "
                f"| partições atuais: {sorted(atual)}"
            )
            return

        self.logger.info(
            f"REBALANCO - partições ATRIBUÍDAS a {self.id_consumidor}: {sorted(novas)} "
            f"| partições atuais: {sorted(atual)}"
        )

    def on_revoke(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado antes de este consumidor perder partições no rebalanço.

        Com cooperative-sticky, só as partições que mudam de dono são revogadas;
        as demais continuam sendo consumidas.

        Args:
            consumidor: Instância do consumidor Kafka que perderá as partições.
            particoes: Partições que estão sendo revogadas.
        """
        revogadas = {p.partition for p in particoes}
        # A revogação também só é aplicada depois que este callback retorna
        restantes = {p.partition for p in consumidor.assignment()} - revogadas
        self.logger.warning(
            f"REBALANCO - partições REVOGADAS de {self.id_consumidor}: {sorted(revogadas)} "
            f"| partições atuais: {sorted(restantes)}"
        )

        try:
            consumidor.commit(asynchronous=False)  # síncrono: tem que terminar antes de devolver
        except KafkaException as e:
            # KafkaError._NO_OFFSET = nada a commitar (ex.: rebalanço logo após a subida)
            if e.args[0].code() != KafkaError._NO_OFFSET:
                self.logger.error(f"Erro ao commitar no rebalanço: {e}")

    def on_lost(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado quando as partições são perdidas sem aviso prévio.

        Ocorre, por exemplo, quando a sessão expira e o coordenador já as
        reatribuiu a outro consumidor.

        Args:
            consumidor: Instância do consumidor Kafka que perdeu as partições.
            particoes: Partições perdidas.
        """
        perdidas = {p.partition for p in particoes}
        restantes = {p.partition for p in consumidor.assignment()} - perdidas
        self.logger.error(
            f"REBALANCO - partições PERDIDAS por {self.id_consumidor}: {sorted(perdidas)} "
            f"| partições atuais: {sorted(restantes)}"
        )

    def _signal_handler(self, signum: int, frame: FrameType | None) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def processar_mensagem(self, dados_mensagem: dict, mensagem: Message | None = None) -> None:
        """Processa a mensagem recebida do Kafka.

        Args:
            dados_mensagem: Dicionário contendo os dados da mensagem.
            mensagem: Objeto de mensagem do Kafka para metadados (opcional).
        """
        sensor_id = dados_mensagem.get("sensor_id")
        setor = dados_mensagem.get("setor")
        timestamp = self._formatar_timestamp(dados_mensagem.get("timestamp"))
        temperatura = dados_mensagem.get("temperatura")
        vibracao = dados_mensagem.get("vibracao")
        umidade = dados_mensagem.get("umidade")
        consumo_energia = dados_mensagem.get("consumo_energia")

        # Verifica se algum parâmetro excede os limites de perigo
        alertas = self._detectar_perigo(dados_mensagem)
        if alertas:
            self.logger.warning(
                "\n".join(self._formatar_alerta(a, dados_mensagem) for a in alertas)
            )

        # Logando os dados recebidos
        particao_str = f"[particao {mensagem.partition()}]" if mensagem else ""
        self.logger.info(
            f"Mensagem recebida - Sensor: {sensor_id}, Setor: {setor}, "
            f"Timestamp: {timestamp}, Temperatura: {temperatura}, "
            f"Vibração: {vibracao}, Umidade: {umidade}, "
            f"Consumo de Energia: {consumo_energia} {particao_str}"
        )

        # Grava em arquivo antes de o offset ser marcado como processado (ver run()).
        # Se a gravação falhar, a exceção sobe e o offset não avança, de modo que a
        # mensagem será reprocessada: é a semântica at-least-once.
        if self.repositorio:
            self.repositorio.gravar_leitura(dados_mensagem, mensagem, bool(alertas))
            if alertas:
                self.repositorio.gravar_alertas(alertas, dados_mensagem, mensagem)

    @staticmethod
    def _formatar_timestamp(timestamp: float | None) -> str:
        """Converte um timestamp Unix em uma data legível.

        Args:
            timestamp: Segundos desde a época Unix, como enviado pelo produtor.

        Returns:
            Data no formato "YYYY-MM-DD HH:MM:SS", ou o valor original como texto
            caso não seja um timestamp válido.
        """
        try:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OverflowError, OSError):
            return str(timestamp)

    def _detectar_perigo(self, dados_mensagem: dict) -> list[dict]:
        """Detecta quais parâmetros do sensor excedem os limites de perigo.

        Args:
            dados_mensagem: Dicionário contendo os dados da mensagem.

        Returns:
            Uma entrada por parâmetro violado, contendo parametro, valor e limite.
            Lista vazia se a leitura estiver dentro de todos os limites.
        """
        limites = self._limites_perigosos()
        alertas = []

        # Verifica cada parâmetro contra seu limite
        for parametro, limite in limites.items():
            if dados_mensagem.get(parametro, 0) > limite:
                alertas.append(
                    {
                        "parametro": parametro,
                        "valor": dados_mensagem.get(parametro),
                        "limite": limite,
                    }
                )
        return alertas

    def _formatar_alerta(self, alerta: dict, dados_mensagem: dict) -> str:
        """Monta a frase usada para registrar um alerta no log.

        Args:
            alerta: Violação detectada, contendo parametro, valor e limite.
            dados_mensagem: Dicionário contendo os dados da mensagem.

        Returns:
            Frase descrevendo o sensor, o parâmetro violado e o limite excedido.
        """
        sensor_id = dados_mensagem.get("sensor_id")
        setor = dados_mensagem.get("setor")
        timestamp = self._formatar_timestamp(dados_mensagem.get("timestamp"))
        return (
            f"ALERTA: Sensor {sensor_id} no setor {setor} excedeu o limite "
            f"de {alerta['parametro']}. Valor: {alerta['valor']}, "
            f"Limite: {alerta['limite']}, Timestamp: {timestamp}."
        )

    def _limites_perigosos(self) -> dict:
        """Retorna os limites de perigo para cada parâmetro do sensor.

        Returns:
            Dicionário com os limites de perigo para cada parâmetro.
        """
        return {
            "temperatura": float(os.getenv("TEMP_LIMITE", "50.0")),
            "vibracao": float(os.getenv("VIBRACAO_LIMITE", "10.0")),
            "umidade": float(os.getenv("UMIDADE_LIMITE", "80.0")),
            "consumo_energia": float(os.getenv("ENERGIA_LIMITE", "200.0")),
        }

    def run(self) -> None:
        """Escuta e processa mensagens do tópico de sensores.

        Executa até ser interrompido.
        """
        self.rodando = True
        self.logger.info(
            f"Inicializando consumidor de sensores: {self.id_consumidor} "
            f"(grupo: {self.grupo_consumidor}, tópico: {self.topico_sensor})"
        )

        try:
            while self.rodando:
                try:
                    # Poll para receber mensagens do Kafka
                    msg = self.consumidor.poll(timeout=1.0)

                    if msg is None:
                        continue  # Nenhuma mensagem recebida, continua o loop

                    if msg.error():
                        self.logger.error(f"Erro ao consumir mensagem: {msg.error()}")
                        continue

                    try:
                        # Decodifica a mensagem recebida e processa
                        dados_mensagem = json.loads(msg.value().decode("utf-8"))

                        # Processa a mensagem recebida
                        self.processar_mensagem(dados_mensagem, msg)

                        # Marca a mensagem como processada --> o commit
                        # em si é feito pela biblioteca a cada 5s
                        self.consumidor.store_offsets(message=msg)

                    except Exception as e:
                        self.logger.error(f"Erro ao processar mensagem: {e}")

                except Exception as e:
                    self.logger.error(f"Erro durante o consumo de mensagens: {e}")

        except Exception as e:
            self.logger.error(f"Erro no loop consumidor: {e}")
        finally:
            self.cleanup()

    def stop(self) -> None:
        """Encerra o consumidor.

        Deve liberar as conexões com o Kafka de forma limpa.
        """
        self.logger.info("Parando o consumidor...")
        self.rodando = False

    def cleanup(self) -> None:
        """Libera recursos do consumidor Kafka e fecha os arquivos de persistência."""
        try:
            self.consumidor.close()
            self.logger.info("Consumidor Kafka encerrado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao encerrar o consumidor Kafka: {e}")

        if self.repositorio:
            self.repositorio.fechar()


def main() -> None:
    """Lê a configuração do ambiente e executa o consumidor.

    Variáveis: CONSUMER_ID, KAFKA_BOOTSTRAP, TOPICO_SENSORES e
    GRUPO_CONSUMIDORES. Todas têm valor padrão.
    """
    # Lê a configuração do ambiente
    # O hostname do container é único por réplica, mesmo com --scale
    id_consumidor = os.getenv("CONSUMER_ID", f"consumidor-{socket.gethostname()}")
    brokers_kafka = os.getenv(
        "KAFKA_BOOTSTRAP",
        "kafka-1-0.kafka-headless:9092,kafka-2-0.kafka-headless:9092,kafka-3-0.kafka-headless:9092",
    )
    topico_sensor = os.getenv("TOPICO_SENSORES", "dados-sensores")
    grupo_consumidor = os.getenv("GRUPO_CONSUMIDORES", "processadores")

    # Cria e executa o consumidor
    consumidor = ConsumidorSensor(
        id_consumidor=id_consumidor,
        brokers_kafka=brokers_kafka,
        topico_sensor=topico_sensor,
        grupo_consumidor=grupo_consumidor,
    )

    try:
        consumidor.run()
    except KeyboardInterrupt:
        consumidor.logger.info("Consumidor interrompido pelo usuário")
    finally:
        consumidor.stop()


if __name__ == "__main__":
    main()
