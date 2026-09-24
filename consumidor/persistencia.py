"""Persistência em arquivo dos dados processados pelo consumidor.

Grava as leituras dos sensores e os alertas em dois arquivos JSON Lines (um objeto
JSON por linha) dentro de um volume compartilhado por todas as réplicas do consumidor.

Como várias réplicas escrevem nos mesmos dois arquivos ao mesmo tempo, a consistência
é obtida sem trava explícita, apoiando-se em duas propriedades combinadas:

1. Os arquivos são abertos em modo append (``"ab"``, que usa ``O_APPEND``). Isso torna
   "posicionar no fim do arquivo e escrever" uma operação indivisível no kernel: duas
   réplicas nunca gravam sobre a mesma região do arquivo.
2. Cada registro é gravado com uma única chamada de escrita, em modo binário sem buffer
   (``buffering=0``). Assim o buffer do Python não parte uma linha em duas chamadas, o
   que permitiria a outra réplica se intercalar no meio de um registro.

O limite portável para essa garantia é ``PIPE_BUF`` (4096 bytes). Os registros deste
sistema têm por volta de 250 bytes; registros maiores são gravados mesmo assim, mas com
um aviso no log.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from confluent_kafka import Message


class RepositorioJsonl:
    """Grava leituras processadas e alertas em arquivos JSON Lines.

    Args:
        diretorio: Diretório onde os arquivos são criados.
        arquivo_dados: Nome do arquivo com as leituras processadas.
        arquivo_alertas: Nome do arquivo com os alertas.
        id_consumidor: Identificador da réplica, gravado em cada registro.
        logger: Logger usado para relatar problemas de gravação.
    """

    LIMITE_ATOMICO = 4096
    """Tamanho máximo (PIPE_BUF), em bytes, com append atômico garantido."""

    def __init__(
        self,
        diretorio: str,
        arquivo_dados: str,
        arquivo_alertas: str,
        id_consumidor: str,
        logger: logging.Logger,
    ) -> None:
        """Abre os arquivos de dados e de alertas em modo append.

        Args:
            diretorio: Diretório onde os arquivos são criados.
            arquivo_dados: Nome do arquivo com as leituras processadas.
            arquivo_alertas: Nome do arquivo com os alertas.
            id_consumidor: Identificador da réplica, gravado em cada registro.
            logger: Logger usado para relatar problemas de gravação.

        Raises:
            OSError: Se o diretório ou os arquivos não puderem ser abertos. O erro é
                propagado de propósito: é melhor o consumidor não subir do que rodar
                silenciosamente sem gravar nada.
        """
        self.id_consumidor = id_consumidor
        self.logger = logger

        pasta = Path(diretorio)
        pasta.mkdir(parents=True, exist_ok=True)

        self.caminho_dados = pasta / arquivo_dados
        self.caminho_alertas = pasta / arquivo_alertas

        # buffering=0: cada write() do Python vira exatamente um write() do sistema.
        # É isso que impede que um registro seja partido e se misture com o de outra réplica.
        self._dados = self.caminho_dados.open("ab", buffering=0)
        self._alertas = self.caminho_alertas.open("ab", buffering=0)

        self.logger.info(
            f"Persistência ativa: {self.caminho_dados} (dados) e {self.caminho_alertas} (alertas)"
        )

    def gravar_leitura(self, dados: dict, mensagem: Message | None, houve_alerta: bool) -> None:
        """Grava uma leitura processada no arquivo de dados.

        Args:
            dados: Dados da mensagem recebida do Kafka.
            mensagem: Mensagem Kafka original, usada para partição e offset.
            houve_alerta: Indica se a leitura violou algum limite.
        """
        registro = self._contexto(dados, mensagem)
        registro.update(
            {
                "temperatura": dados.get("temperatura"),
                "vibracao": dados.get("vibracao"),
                "umidade": dados.get("umidade"),
                "consumo_energia": dados.get("consumo_energia"),
                "alerta": houve_alerta,
            }
        )
        self._escrever(self._dados, registro)

    def gravar_alertas(self, alertas: list[dict], dados: dict, mensagem: Message | None) -> None:
        """Grava um registro por parâmetro violado no arquivo de alertas.

        Args:
            alertas: Violações detectadas, cada uma com parametro, valor e limite.
            dados: Dados da mensagem recebida do Kafka.
            mensagem: Mensagem Kafka original, usada para partição e offset.
        """
        for alerta in alertas:
            registro = self._contexto(dados, mensagem)
            registro.update(alerta)
            self._escrever(self._alertas, registro)

    def _contexto(self, dados: dict, mensagem: Message | None) -> dict:
        """Monta os campos de origem comuns aos dois tipos de registro.

        A partição e o offset identificam de forma única a mensagem no tópico. Como a
        gravação acontece antes do commit do offset (semântica *at-least-once*), um
        rebalanceamento pode fazer a mesma leitura ser gravada duas vezes; o par
        (particao, offset) é o que permite reconhecer essas duplicatas na análise.

        Args:
            dados: Dados da mensagem recebida do Kafka.
            mensagem: Mensagem Kafka original, usada para partição e offset.

        Returns:
            Dicionário com quem gravou o registro, de onde ele veio e quando.
        """
        return {
            "gravado_em": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "consumidor": self.id_consumidor,
            "particao": mensagem.partition() if mensagem else None,
            "offset": mensagem.offset() if mensagem else None,
            "sensor_id": dados.get("sensor_id"),
            "setor": dados.get("setor"),
            "timestamp": dados.get("timestamp"),
        }

    def _escrever(self, handle: BinaryIO, registro: dict) -> None:
        """Grava um registro como uma linha, em uma única chamada de escrita.

        Args:
            handle: Arquivo aberto em modo binário sem buffer.
            registro: Registro a ser serializado.
        """
        linha = json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n"
        bruto = linha.encode("utf-8")

        if len(bruto) > self.LIMITE_ATOMICO:
            self.logger.warning(
                f"Registro de {len(bruto)} bytes excede o PIPE_BUF ({self.LIMITE_ATOMICO}): "
                f"o append atômico não é garantido e a linha pode se misturar com outra"
            )

        escrito = handle.write(bruto)
        if escrito != len(bruto):
            self.logger.error(
                f"Escrita parcial: {escrito} de {len(bruto)} bytes gravados. "
                f"A linha pode ter ficado incompleta no arquivo"
            )

    def fechar(self) -> None:
        """Fecha os arquivos de dados e de alertas."""
        for handle in (self._dados, self._alertas):
            try:
                handle.close()
            except OSError as e:
                self.logger.error(f"Erro ao fechar arquivo de persistência: {e}")
