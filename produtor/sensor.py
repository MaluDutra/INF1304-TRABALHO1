"""sensor.py.

Simula um sensor de uma máquina da fábrica inteligente.
Gera periodicamente uma leitura (temperatura, vibração, consumo de
energia e umidade) e publica no tópico Kafka configurado, como produtor.
"""

import json
import logging
import os
import random
import socket
import time

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------
# Configuração
# --------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPICO = os.environ["TOPICO_SENSORES"]
INTERVALO = float(os.environ.get("INTERVALO_SEGUNDOS", "2"))

SENSOR_ID = os.environ.get("SENSOR_ID", socket.gethostname())
SETOR = os.environ["SETOR"]  # setor fixo da máquina

# O nível de log pode ser configurado via variável de ambiente LOG_LEVEL
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(SENSOR_ID)

# Faixas de simulação: definem os valores mínimos e máximos que o
# sensor pode reportar.
FAIXAS = {
    "temperatura": (float(os.environ["TEMP_MIN"]), float(os.environ["TEMP_MAX"])),
    "vibracao": (float(os.environ["VIBRACAO_MIN"]), float(os.environ["VIBRACAO_MAX"])),
    "umidade": (float(os.environ["UMIDADE_MIN"]), float(os.environ["UMIDADE_MAX"])),
    "consumo_energia": (float(os.environ["ENERGIA_MIN"]), float(os.environ["ENERGIA_MAX"])),
}


def gerar_leitura(sensor_id: str) -> dict:
    """Gera uma leitura simulada de um sensor da fábrica.

    Cada grandeza é sorteada dentro da faixa configurada, de modo que valores
    acima dos limites de alerta ocorram com alguma frequência.

    Args:
        sensor_id: Identificador do sensor que gerou a leitura.

    Returns:
        Dicionário com as grandezas medidas, o setor e o instante da medição.
    """
    leitura = {
        "sensor_id": sensor_id,
        "setor": SETOR,
        "timestamp": time.time(),
    }

    for grandeza, (minimo, maximo) in FAIXAS.items():
        leitura[grandeza] = round(random.uniform(minimo, maximo), 2)

    return leitura


def callback_entrega(erro: KafkaError | None, msg: Message) -> None:
    """Callback de entrega de mensagens do Kafka.

    Callback chamado pelo cliente Kafka de forma assíncrona, informando se
    a mensagem foi confirmada pelo broker ou falhou.

    Args:
        erro: Objeto de erro do Kafka, ou None se deu certo.
        msg: A mensagem original que foi enviada.
    """
    if erro is not None:
        logger.error(f"Falha ao entregar mensagem: {erro}")
    else:
        logger.info(f"{msg.topic()} partição={msg.partition()} offset={msg.offset()}")


def main() -> None:
    """Script principal do sensor.

    Loop principal do produtor: cria o cliente Kafka e envia leituras
    simuladas continuamente até ser interrompido.
    """
    produtor = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": SENSOR_ID,
        }
    )

    logger.info(f"[{SENSOR_ID}] iniciando, enviando para '{TOPICO}' a cada {INTERVALO}s")

    try:
        while True:
            leitura = gerar_leitura(SENSOR_ID)

            try:
                produtor.produce(
                    topic=TOPICO,
                    key=SENSOR_ID,
                    value=json.dumps(leitura),
                    callback=callback_entrega,
                )
            except KafkaException as e:
                # Tópico ainda não criado ou cluster indisponível:
                # o sensor continua medindo e tenta novamente depois.
                logger.error(f"[{SENSOR_ID}] erro ao publicar, tentando de novo: {e}")
            except BufferError:
                # Fila interna cheia (broker fora do ar): aguarda espaço
                logger.warning("[{SENSOR_ID}] fila cheia, aguardando...")
                produtor.poll(1)

            produtor.poll(0)
            logger.info(f"[{SENSOR_ID}] enviado: {leitura}")
            time.sleep(INTERVALO)

    except KeyboardInterrupt:
        logger.info("[{SENSOR_ID}] encerrando...")
    finally:
        produtor.flush()  # garante que tudo que estava na fila é enviado


if __name__ == "__main__":
    main()
