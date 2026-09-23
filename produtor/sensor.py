"""
sensor.py

Simula um sensor de uma máquina da fábrica inteligente.
Gera periodicamente uma leitura (temperatura, vibração, consumo de
energia e umidade) e publica no tópico Kafka configurado, como produtor.
"""

import json
import os
import random
import time
import socket

from confluent_kafka import Producer, KafkaException
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

# Faixas de simulação: definem os valores mínimos e máximos que o
# sensor pode reportar.
FAIXAS = {
    "temperatura": (float(os.environ["TEMP_MIN"]), float(os.environ["TEMP_MAX"])),
    "vibracao": (float(os.environ["VIBRACAO_MIN"]), float(os.environ["VIBRACAO_MAX"])),
    "umidade": (float(os.environ["UMIDADE_MIN"]), float(os.environ["UMIDADE_MAX"])),
    "consumo_energia": (float(os.environ["ENERGIA_MIN"]), float(os.environ["ENERGIA_MAX"])),
}


def gerar_leitura(sensor_id: str) -> dict:
    """
    Gera uma leitura simulada de um sensor da fábrica.

    Cada grandeza é sorteada dentro da faixa configurada,
    de modo que valores acima dos limites de alerta
    ocorram com alguma frequência.

    :param sensor_id: identificador do sensor que gerou a leitura.
    :return: dicionário com as grandezas medidas, o setor e o
             instante da medição.
    """
    leitura = {
        "sensor_id": sensor_id,
        "setor": SETOR,
        "timestamp": time.time(),
    }

    for grandeza, (minimo, maximo) in FAIXAS.items():
        leitura[grandeza] = round(random.uniform(minimo, maximo), 2)

    return leitura


def callback_entrega(erro, msg):
    """
    Callback chamado pelo cliente Kafka de forma assíncrona,
    informando se a mensagem foi confirmada pelo broker ou falhou.

    :param erro: objeto de erro do Kafka, ou None se deu certo.
    :param msg: a mensagem original que foi enviada.
    """
    if erro is not None:
        print(f"[ERRO] Falha ao entregar mensagem: {erro}")
    else:
        print(f"[OK] {msg.topic()} partição={msg.partition()} offset={msg.offset()}")


def main():
    """
    Loop principal do produtor: cria o cliente Kafka e envia
    leituras simuladas continuamente até ser interrompido.
    """
    produtor = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": SENSOR_ID,
        }
    )

    print(f"[{SENSOR_ID}] iniciando, enviando para '{TOPICO}' a cada {INTERVALO}s")

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
                print(f"[{SENSOR_ID}] erro ao publicar, tentando de novo: {e}")
            except BufferError:
                # Fila interna cheia (broker fora do ar): aguarda espaço
                print(f"[{SENSOR_ID}] fila cheia, aguardando...")
                produtor.poll(1)

            produtor.poll(0)
            print(f"[{SENSOR_ID}] enviado: {leitura}")
            time.sleep(INTERVALO)      

    except KeyboardInterrupt:
        print(f"[{SENSOR_ID}] encerrando...")
    finally:
        produtor.flush()  # garante que tudo que estava na fila é enviado


if __name__ == "__main__":
    main()
