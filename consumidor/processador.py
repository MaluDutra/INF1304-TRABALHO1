"""Processador de dados de sensores Kafka."""

import asyncio
import json
import logging
import os
import time

from confluent_kafka import Consumer


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
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }

        try:
            self.consumidor = Consumer(consumidor_config)
            self.logger.info("Consumidor Kafka inicializado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar o consumidor Kafka: {e}")
            raise

        # Assinatura do tópico de sensores
        self.consumidor.subscribe([self.topico_sensor])
        self.logger.info(f"Assinando tópico: {self.topico_sensor}")

    def _configurar_logger(self) -> logging.Logger:
        """Configuração do logger."""
        # O nível de log pode ser configurado via variável de ambiente LOG_LEVEL
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        return logging.getLogger(self.id_consumidor)

    def processar_mensagem(self, dados_mensagem: dict) -> None:
        """Processa a mensagem recebida do Kafka.

        Args:
            dados_mensagem: Dicionário contendo os dados da mensagem.
        """
        # Aqui você pode implementar a lógica de processamento dos dados do sensor
        self.logger.info(f"Mensagem recebida: {dados_mensagem}")

    async def run(self) -> None:
        """Escuta e processa mensagens do tópico de sensores.

        Executa até ser interrompido.
        """
        self.rodando = True
        self.logger.info(
            f"Inicializando consumidor de sensores: {self.id_consumidor} "
            f"(grupo: {self.grupo_consumidor}, tópico: {self.topico_sensor})"
        )

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
                    self.processar_mensagem(dados_mensagem)

                except Exception as e:
                    self.logger.error(f"Erro ao processar mensagem: {e}")

                # Pequena pausa para evitar sobrecarga do loop
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Erro durante o consumo de mensagens: {e}")

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
    id_consumidor = os.getenv("CONSUMER_ID", f"consumidor-{int(time.time())}")
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
