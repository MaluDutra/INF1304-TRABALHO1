"""Processador de dados de sensores Kafka."""

import asyncio
import json
import logging
import os
import signal
import socket
from datetime import datetime
from types import FrameType

from confluent_kafka import Consumer, TopicPartition


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

        # Configuração do consumidor Kafka
        consumidor_config = {
            "bootstrap.servers": self.brokers_kafka,
            "group.id": self.grupo_consumidor,
            "client.id": self.id_consumidor,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
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

    def on_assign(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado quando o coordenador atribui partições a este consumidor.

        Apenas registra o evento no log; a atribuição em si é feita pela biblioteca.

        Args:
            consumidor: Instância do consumidor Kafka que recebeu as partições.
            particoes: Partições atribuídas a este consumidor.
        """
        ids = sorted(p.partition for p in particoes)
        self.logger.info(
            f"REBALANCO - partições ATRIBUÍDAS a {self.id_consumidor} "
            f"(member.id={consumidor.memberid()}): {ids}"
        )

    def on_revoke(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado antes de este consumidor perder partições no rebalanço.

        Args:
            consumidor: Instância do consumidor Kafka que perderá as partições.
            particoes: Partições que estão sendo revogadas.
        """
        ids = sorted(p.partition for p in particoes)
        self.logger.warning(f"REBALANCO - partições REVOGADAS de {self.id_consumidor}: {ids}")

    def on_lost(self, consumidor: Consumer, particoes: list[TopicPartition]) -> None:
        """Callback disparado quando as partições são perdidas sem aviso prévio.

        Ocorre, por exemplo, quando a sessão expira e o coordenador já as
        reatribuiu a outro consumidor.

        Args:
            consumidor: Instância do consumidor Kafka que perdeu as partições.
            particoes: Partições perdidas.
        """
        ids = sorted(p.partition for p in particoes)
        self.logger.error(f"REBALANCO - partições PERDIDAS por {self.id_consumidor}: {ids}")

    def _signal_handler(self, signum: int, frame: FrameType | None) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def processar_mensagem(self, dados_mensagem: dict, mensagem: dict | None = None) -> None:
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
        alerta = self._detectar_perigo(dados_mensagem)
        if alerta:
            self.logger.warning(f"ALERTA: {alerta}")

        # Logando os dados recebidos
        particao_str = f"[particao {mensagem.partition()}]" if mensagem else ""
        self.logger.info(
            f"Mensagem recebida - Sensor: {sensor_id}, Setor: {setor}, "
            f"Timestamp: {timestamp}, Temperatura: {temperatura}, "
            f"Vibração: {vibracao}, Umidade: {umidade}, "
            f"Consumo de Energia: {consumo_energia} {particao_str}"
        )

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

    def _detectar_perigo(self, dados_mensagem: dict) -> str | None:
        """Detecta se algum parâmetro do sensor excede os limites de perigo.

        Args:
            sensor_id: Identificador do sensor.
            dados_mensagem: Dicionário contendo os dados da mensagem.

        Returns:
            Mensagem de alerta se algum parâmetro exceder o limite, ou None caso contrário.
        """
        limites = self._limites_perigosos()

        # Verifica cada parâmetro contra seu limite
        for parametro, limite in limites.items():
            if dados_mensagem.get(parametro) > limite:
                valor = dados_mensagem.get(parametro)
                sensor_id = dados_mensagem.get("sensor_id")
                setor = dados_mensagem.get("setor")
                timestamp = self._formatar_timestamp(dados_mensagem.get("timestamp"))
                return (
                    f"Sensor {sensor_id} no setor {setor} excedeu o limite "
                    f"de {parametro}. Valor: {valor}, Limite: {limite}, "
                    f"Timestamp: {timestamp}"
                )
        return None

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

    async def run(self) -> None:
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

                    except Exception as e:
                        self.logger.error(f"Erro ao processar mensagem: {e}")

                    # Pequena pausa para evitar sobrecarga do loop
                    await asyncio.sleep(0.1)

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
        """Libera recursos do consumidor Kafka."""
        try:
            self.consumidor.close()
            self.logger.info("Consumidor Kafka encerrado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao encerrar o consumidor Kafka: {e}")


async def main() -> None:
    """Lê a configuração do ambiente e executa o consumidor.

    Variáveis: CONSUMER_ID, KAFKA_BOOTSTRAP, TOPICO_SENSORES e
    GRUPO_CONSUMIDORES. Todas têm valor padrão.
    """
    # Lê a configuração do ambiente
    # O hostname do container é único por réplica, mesmo com --scale
    id_consumidor = os.getenv("CONSUMER_ID", f"consumidor-{socket.gethostname()}")
    brokers_kafka = os.getenv("KAFKA_BOOTSTRAP", "kafka1:19092,kafka2:19092,kafka3:19092")
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
        await consumidor.run()
    except KeyboardInterrupt:
        consumidor.logger.info("Consumidor interrompido pelo usuário")
    finally:
        consumidor.stop()


if __name__ == "__main__":
    asyncio.run(main())
