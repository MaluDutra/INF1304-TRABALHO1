"""Gerador do dashboard HTML da Fábrica Inteligente.

Lê os arquivos JSON Lines gravados pelos consumidores, agrega os números e escreve
um único arquivo HTML autocontido: sem CDN, sem servidor e sem biblioteca de
terceiros, de modo que a página abra offline e possa ser anexada ao relatório.

Quando o cluster está no ar, o dashboard ganha um painel com o estado ao vivo
(réplicas, HPA e lag por partição). Se o cluster estiver fora, a página é gerada
do mesmo jeito, apenas sem esse painel.

Nenhum valor é fixo no código: os nomes dos arquivos, os limites de perigo e os
nomes do tópico e do grupo vêm das mesmas variáveis de ambiente que o consumidor
usa, de modo que o limite desenhado no gráfico é exatamente o que gerou o alerta.
"""

import json
import logging
import os
import subprocess
from collections import Counter, defaultdict
from datetime import UTC, datetime
from html import escape
from pathlib import Path

logger = logging.getLogger("dashboard")

# Paleta categórica validada (claro, escuro). A ordem é o mecanismo de segurança
# para daltonismo: os pares adjacentes foram verificados, então não reordene.
PALETA = [
    ("#2a78d6", "#3987e5"),
    ("#eb6834", "#d95926"),
    ("#1baf7a", "#199e70"),
    ("#eda100", "#c98500"),
    ("#e87ba4", "#d55181"),
    ("#008300", "#008300"),
    ("#4a3aa7", "#9085e9"),
    ("#e34948", "#e66767"),
]

PARAMETROS = [
    ("temperatura", "Temperatura", "°C", "TEMP_LIMITE", "50.0"),
    ("vibracao", "Vibração", "mm/s", "VIBRACAO_LIMITE", "10.0"),
    ("umidade", "Umidade", "%", "UMIDADE_LIMITE", "80.0"),
    ("consumo_energia", "Consumo de energia", "W", "ENERGIA_LIMITE", "200.0"),
]

BALDES = 48
MAX_SERIES = 8
# Um nome de pod tem a forma consumidor-<replicaset>-<sufixo>; acima disso há sufixo a extrair.
PARTES_NOME_POD = 2
# Colunas mínimas na saída do kafka-consumer-groups.sh para a linha ser aproveitável.
COLUNAS_LAG = 7
# Com uma só série a legenda apenas repetiria o título do gráfico.
MIN_SERIES_LEGENDA = 2


def carregar_jsonl(caminho: Path) -> list[dict]:
    """Lê um arquivo JSON Lines, ignorando linhas ilegíveis.

    Args:
        caminho: Arquivo a ser lido.

    Returns:
        Os registros lidos. Lista vazia se o arquivo não existir.
    """
    if not caminho.exists():
        logger.warning(f"Arquivo não encontrado: {caminho}")
        return []

    registros = []
    invalidas = 0
    with caminho.open(encoding="utf-8") as arquivo:
        for linha in arquivo:
            if not linha.strip():
                continue
            try:
                registros.append(json.loads(linha))
            except json.JSONDecodeError:
                invalidas += 1

    if invalidas:
        logger.warning(f"{caminho.name}: {invalidas} linhas ilegíveis foram ignoradas")
    logger.info(f"{caminho.name}: {len(registros)} registros")
    return registros


def _momento(registro: dict) -> datetime | None:
    """Converte o campo gravado_em de um registro em datetime.

    Args:
        registro: Registro lido do arquivo JSON Lines.

    Returns:
        O instante da gravação, ou None se o campo estiver ausente ou inválido.
    """
    try:
        return datetime.fromisoformat(registro["gravado_em"])
    except (KeyError, TypeError, ValueError):
        return None


def rotulo_curto(nome: str) -> str:
    """Encurta o nome de um pod para caber nos rótulos dos gráficos.

    Args:
        nome: Nome completo do consumidor, como "consumidor-697d85767-jxsbk".

    Returns:
        O sufixo que identifica o pod, ou o nome inteiro se ele já for curto.
    """
    partes = nome.split("-")
    return partes[-1] if len(partes) > PARTES_NOME_POD else nome


def agregar(leituras: list[dict], alertas: list[dict]) -> dict:
    """Resume as leituras e os alertas nos números que o dashboard exibe.

    Args:
        leituras: Registros do arquivo de dados processados.
        alertas: Registros do arquivo de alertas.

    Returns:
        Dicionário com contagens por consumidor, por partição, a matriz
        consumidor por partição, os baldes de tempo e as séries dos sensores.
    """
    momentos = [m for m in (_momento(r) for r in leituras) if m]
    inicio, fim = (min(momentos), max(momentos)) if momentos else (None, None)
    duracao = (fim - inicio).total_seconds() if inicio and fim else 0.0

    por_consumidor = Counter(r.get("consumidor", "?") for r in leituras)
    consumidores = [nome for nome, _ in por_consumidor.most_common(MAX_SERIES)]

    chaves = Counter((r.get("particao"), r.get("offset")) for r in leituras)
    duplicatas = sum(n - 1 for n in chaves.values() if n > 1)

    return {
        "total_leituras": len(leituras),
        "total_alertas": len(alertas),
        "leituras_com_alerta": sum(1 for r in leituras if r.get("alerta")),
        "duplicatas": duplicatas,
        "inicio": inicio,
        "fim": fim,
        "duracao": duracao,
        "vazao": len(leituras) / duracao if duracao > 0 else 0.0,
        "consumidores": consumidores,
        "por_consumidor": por_consumidor,
        "por_particao": Counter(r.get("particao") for r in leituras),
        "matriz": _matriz(leituras),
        "por_parametro": Counter(a.get("parametro") for a in alertas),
        "por_setor": Counter(a.get("setor") for a in alertas),
        "por_sensor": Counter(r.get("sensor_id") for r in leituras),
        "baldes": _baldes_por_consumidor(leituras, consumidores, inicio, fim),
        "series": _series_sensores(leituras, inicio, fim),
        "ultimos_alertas": alertas[-20:][::-1],
    }


def _matriz(leituras: list[dict]) -> dict:
    """Conta quantas leituras cada consumidor processou de cada partição.

    Args:
        leituras: Registros do arquivo de dados processados.

    Returns:
        Dicionário indexado por (consumidor, partição) com a contagem.
    """
    matriz: dict = defaultdict(int)
    for r in leituras:
        matriz[(r.get("consumidor", "?"), r.get("particao"))] += 1
    return dict(matriz)


def _indice_balde(momento: datetime, inicio: datetime, span: float) -> int:
    """Descobre em qual balde de tempo um instante cai.

    Args:
        momento: Instante a posicionar.
        inicio: Começo da janela coberta pelos dados.
        span: Duração total da janela, em segundos.

    Returns:
        O índice do balde, entre 0 e BALDES - 1.
    """
    posicao = (momento - inicio).total_seconds() / span
    return min(int(posicao * BALDES), BALDES - 1)


def _baldes_por_consumidor(
    leituras: list[dict],
    consumidores: list[str],
    inicio: datetime | None,
    fim: datetime | None,
) -> list[list[int]]:
    """Distribui as leituras em baldes de tempo, separadas por consumidor.

    É esta agregação que revela o failover: a faixa de um consumidor termina e a
    de outro começa no mesmo ponto da linha do tempo.

    Args:
        leituras: Registros do arquivo de dados processados.
        consumidores: Consumidores que ganham uma faixa própria.
        inicio: Começo da janela coberta pelos dados.
        fim: Fim da janela coberta pelos dados.

    Returns:
        Uma lista por consumidor, cada uma com BALDES contagens.
    """
    grade = [[0] * BALDES for _ in consumidores]
    if not inicio or not fim:
        return grade

    span = max((fim - inicio).total_seconds(), 1e-9)
    posicao = {nome: i for i, nome in enumerate(consumidores)}
    for r in leituras:
        indice = posicao.get(r.get("consumidor", "?"))
        momento = _momento(r)
        if indice is None or not momento:
            continue
        grade[indice][_indice_balde(momento, inicio, span)] += 1
    return grade


def _series_sensores(
    leituras: list[dict], inicio: datetime | None, fim: datetime | None
) -> dict:
    """Calcula a média de cada parâmetro do sensor por balde de tempo.

    Args:
        leituras: Registros do arquivo de dados processados.
        inicio: Começo da janela coberta pelos dados.
        fim: Fim da janela coberta pelos dados.

    Returns:
        Dicionário com uma lista de médias (ou None em baldes vazios) por parâmetro.
    """
    somas = {chave: [0.0] * BALDES for chave, *_ in PARAMETROS}
    contagens = {chave: [0] * BALDES for chave, *_ in PARAMETROS}
    if not inicio or not fim:
        return {chave: [None] * BALDES for chave, *_ in PARAMETROS}

    span = max((fim - inicio).total_seconds(), 1e-9)
    for r in leituras:
        momento = _momento(r)
        if not momento:
            continue
        balde = _indice_balde(momento, inicio, span)
        for chave, *_ in PARAMETROS:
            valor = r.get(chave)
            if isinstance(valor, (int, float)):
                somas[chave][balde] += valor
                contagens[chave][balde] += 1

    return {
        chave: [
            somas[chave][i] / contagens[chave][i] if contagens[chave][i] else None
            for i in range(BALDES)
        ]
        for chave, *_ in PARAMETROS
    }


def _kubectl(argumentos: list[str], timeout: int = 8) -> str | None:
    """Executa um comando kubectl sem deixar falha derrubar o gerador.

    Args:
        argumentos: Argumentos passados ao kubectl.
        timeout: Tempo máximo de espera, em segundos.

    Returns:
        A saída padrão do comando, ou None se ele falhar por qualquer motivo.
    """
    try:
        resultado = subprocess.run(  # noqa: S603
            ["kubectl", *argumentos],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return resultado.stdout


def _json_kubectl(argumentos: list[str]) -> dict | None:
    """Executa um kubectl que devolve JSON e converte o resultado.

    Args:
        argumentos: Argumentos passados ao kubectl.

    Returns:
        O objeto decodificado, ou None se o comando ou a decodificação falhar.
    """
    saida = _kubectl(argumentos)
    if not saida:
        return None
    try:
        return json.loads(saida)
    except json.JSONDecodeError:
        return None


def _lag_por_particao(namespace: str, broker: str) -> list[dict]:
    """Lê o lag de cada partição executando o utilitário do Kafka num broker.

    Args:
        namespace: Namespace onde os pods estão.
        broker: Pod do broker usado para rodar o utilitário.

    Returns:
        Uma entrada por partição, com offset, fim do log, lag e consumidor.
    """
    saida = _kubectl(
        [
            "exec", "-n", namespace, broker, "--",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", os.getenv("BOOTSTRAP", "kafka-1-0.kafka-headless:9092"),
            "--describe", "--group", os.getenv("GRUPO_CONSUMIDORES", "processadores"),
        ],
        timeout=30,
    )
    if not saida:
        return []

    linhas = []
    for linha in saida.splitlines():
        campos = linha.split()
        if len(campos) < COLUNAS_LAG or campos[0] == "GROUP":
            continue
        try:
            linhas.append(
                {
                    "particao": int(campos[2]),
                    "offset": campos[3],
                    "fim": campos[4],
                    "lag": campos[5],
                    "consumidor": campos[6],
                }
            )
        except (ValueError, IndexError):
            continue
    return sorted(linhas, key=lambda x: x["particao"])


def consultar_cluster(namespace: str) -> dict | None:
    """Coleta o estado ao vivo do cluster, se ele estiver acessível.

    Args:
        namespace: Namespace onde a aplicação está implantada.

    Returns:
        Dicionário com réplicas, dados do HPA e lag por partição, ou None quando
        o cluster não responde ou o Deployment não existe.
    """
    deploy = _json_kubectl(["get", "deployment", "consumidor", "-n", namespace, "-o", "json"])
    if not deploy:
        logger.warning("Cluster indisponível: o dashboard usará apenas os arquivos")
        return None

    hpa = _json_kubectl(["get", "hpa", "consumidor-hpa", "-n", namespace, "-o", "json"]) or {}
    pods = _json_kubectl(
        ["get", "pods", "-l", "app=consumidor", "-n", namespace, "-o", "json"]
    ) or {}

    estado_hpa = hpa.get("status", {})
    metricas = estado_hpa.get("currentMetrics") or []
    cpu = None
    if metricas:
        cpu = metricas[0].get("resource", {}).get("current", {}).get("averageUtilization")

    return {
        "replicas_desejadas": deploy.get("spec", {}).get("replicas"),
        "replicas_prontas": deploy.get("status", {}).get("readyReplicas", 0),
        "hpa_min": hpa.get("spec", {}).get("minReplicas"),
        "hpa_max": hpa.get("spec", {}).get("maxReplicas"),
        "hpa_atual": estado_hpa.get("currentReplicas"),
        "cpu": cpu,
        "pods": [
            {
                "nome": p.get("metadata", {}).get("name", "?"),
                "fase": p.get("status", {}).get("phase", "?"),
            }
            for p in pods.get("items", [])
        ],
        "lag": _lag_por_particao(namespace, os.getenv("BROKER", "kafka-1-0")),
    }


def num(valor: float | int | None, casas: int = 0) -> str:
    """Formata um número no padrão brasileiro, com ponto de milhar.

    Args:
        valor: Número a formatar.
        casas: Casas decimais desejadas.

    Returns:
        O número formatado, ou um travessão se o valor for None.
    """
    if valor is None:
        return "—"
    texto = f"{valor:,.{casas}f}"
    return texto.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def _caminho_barra(x: float, y: float, largura: float, altura: float, raio: float = 4) -> str:
    """Desenha uma barra com a ponta arredondada e a base reta.

    Args:
        x: Posição horizontal da base.
        y: Posição vertical do topo da barra.
        largura: Comprimento da barra.
        altura: Espessura da barra.
        raio: Raio do arredondamento da ponta.

    Returns:
        O atributo "d" de um elemento path SVG.
    """
    if largura <= 0:
        return ""
    raio = min(raio, largura, altura / 2)
    return (
        f"M{x:.1f},{y:.1f} H{x + largura - raio:.1f} "
        f"Q{x + largura:.1f},{y:.1f} {x + largura:.1f},{y + raio:.1f} "
        f"V{y + altura - raio:.1f} "
        f"Q{x + largura:.1f},{y + altura:.1f} {x + largura - raio:.1f},{y + altura:.1f} "
        f"H{x:.1f} Z"
    )


def barras_horizontais(
    itens: list[tuple[str, int]],
    slot: int = 1,
    largura: int = 560,
    por_item: bool = False,
) -> str:
    """Monta um gráfico de barras horizontais como SVG.

    Args:
        itens: Pares (rótulo, valor), já na ordem desejada.
        slot: Índice da cor categórica usada nas barras.
        largura: Largura total do gráfico, em pixels.
        por_item: Se verdadeiro, cada barra recebe a cor da sua posição em vez de
            uma cor única. Usado quando as barras são entidades que também aparecem
            coloridas em outro painel, para que a cor siga a entidade.

    Returns:
        O elemento svg completo, ou um aviso se não houver dados.
    """
    if not itens:
        return '<p class="vazio">Sem dados para exibir.</p>'

    espessura, espaco, col_rotulo, col_valor = 22, 12, 150, 70
    maximo = max(v for _, v in itens) or 1
    trilho = largura - col_rotulo - col_valor
    altura = len(itens) * (espessura + espaco) + espaco

    partes = [
        f'<svg viewBox="0 0 {largura} {altura}" width="100%" height="{altura}" '
        f'role="img" class="gr">'
    ]
    for i, (rotulo, valor) in enumerate(itens):
        y = espaco + i * (espessura + espaco)
        comprimento = trilho * valor / maximo
        dica = escape(f"{rotulo}: {num(valor)} registros")
        cor = i % MAX_SERIES + 1 if por_item else slot
        partes.append(
            f'<g class="marca" data-tip="{dica}">'
            f'<text x="{col_rotulo - 10}" y="{y + espessura / 2 + 4}" '
            f'text-anchor="end" class="rot">{escape(rotulo[:22])}</text>'
            f'<path d="{_caminho_barra(col_rotulo, y, comprimento, espessura)}" '
            f'fill="var(--serie-{cor})"/>'
            f'<text x="{col_rotulo + comprimento + 8}" y="{y + espessura / 2 + 4}" '
            f'class="val">{num(valor)}</text>'
            f"</g>"
        )
    partes.append("</svg>")
    return "".join(partes)


def _escala_agradavel(maximo: float, alvo: int = 4) -> tuple[float, float]:
    """Escolhe um teto e um passo redondos para o eixo vertical.

    Args:
        maximo: Maior valor que precisa caber no eixo.
        alvo: Quantidade aproximada de marcas desejada.

    Returns:
        O teto do eixo e o intervalo entre as marcas.
    """
    if maximo <= 0:
        return 1.0, 1.0
    bruto = maximo / alvo
    magnitude = 10 ** int(f"{bruto:e}".split("e")[1])
    for multiplo in (1, 2, 2.5, 5, 10):
        passo = magnitude * multiplo
        if passo >= bruto:
            break
    return passo * (int(maximo / passo) + 1), passo


def _eixo_y(topo: float, passo: float, x: float, base: float, alto: float) -> str:
    """Desenha as marcas e as linhas de grade do eixo vertical.

    Args:
        topo: Valor no alto do eixo.
        passo: Intervalo entre as marcas.
        x: Posição horizontal onde os rótulos terminam.
        base: Coordenada vertical do zero.
        alto: Altura útil da área de plotagem.

    Returns:
        Os elementos SVG da grade e dos rótulos.
    """
    partes = []
    valor = 0.0
    while valor <= topo + 1e-9:
        y = base - (valor / topo) * alto
        partes.append(
            f'<line x1="{x + 8}" y1="{y:.1f}" x2="100%" y2="{y:.1f}" class="grade"/>'
            f'<text x="{x}" y="{y + 4:.1f}" text-anchor="end" class="tick">'
            f"{num(valor)}</text>"
        )
        valor += passo
    return "".join(partes)


def colunas_empilhadas(
    grade: list[list[int]],
    consumidores: list[str],
    janela: tuple[datetime | None, datetime | None],
    largura: int = 900,
    altura: int = 280,
) -> str:
    """Monta a linha do tempo de leituras, empilhada por consumidor.

    Uma faixa que termina e outra que começa no mesmo ponto é o failover; faixas
    que surgem ao longo do tempo são a elasticidade do HPA entrando em ação.

    Args:
        grade: Contagens por consumidor e por balde de tempo.
        consumidores: Nomes dos consumidores, na ordem das faixas.
        janela: Início e fim do período coberto pelos dados.
        largura: Largura total do gráfico, em pixels.
        altura: Altura total do gráfico, em pixels.

    Returns:
        O elemento svg completo, ou um aviso se não houver dados.
    """
    if not grade or not any(any(linha) for linha in grade):
        return '<p class="vazio">Sem dados para exibir.</p>'

    esq, dir_, topo_m, baixo = 56, 12, 14, 30
    alto = altura - topo_m - baixo
    base = topo_m + alto
    util = largura - esq - dir_
    fatia = util / len(grade[0])
    espessura = min(fatia - 3, 24)

    totais = [sum(linha[i] for linha in grade) for i in range(len(grade[0]))]
    topo, passo = _escala_agradavel(max(totais))

    partes = [
        f'<svg viewBox="0 0 {largura} {altura}" width="100%" height="{altura}" '
        f'role="img" class="gr">',
        _eixo_y(topo, passo, esq - 8, base, alto),
    ]

    for coluna in range(len(grade[0])):
        x = esq + coluna * fatia + (fatia - espessura) / 2
        acumulado = 0.0
        for serie, nome in enumerate(consumidores):
            valor = grade[serie][coluna]
            if not valor:
                continue
            h = (valor / topo) * alto
            y = base - acumulado - h
            # Folga de 2px na cor da superfície separa os segmentos empilhados.
            visivel = max(h - 2, 1)
            dica = escape(f"{rotulo_curto(nome)}: {num(valor)} leituras")
            partes.append(
                f'<rect class="marca" data-tip="{dica}" x="{x:.1f}" y="{y:.1f}" '
                f'width="{espessura:.1f}" height="{visivel:.1f}" '
                f'fill="var(--serie-{serie % MAX_SERIES + 1})"/>'
            )
            acumulado += h

    inicio, fim = janela
    if inicio and fim:
        partes.append(
            f'<text x="{esq}" y="{altura - 8}" class="tick">'
            f'{inicio.astimezone().strftime("%H:%M:%S")}</text>'
            f'<text x="{largura - dir_}" y="{altura - 8}" text-anchor="end" class="tick">'
            f'{fim.astimezone().strftime("%H:%M:%S")}</text>'
        )
    partes.append("</svg>")
    return "".join(partes)


def serie_com_limite(
    valores: list[float | None],
    limite: float,
    unidade: str,
    slot: int,
    largura: int = 430,
    altura: int = 190,
) -> str:
    """Monta a série temporal de um parâmetro, com a linha de limite.

    O limite desenhado vem da mesma variável de ambiente que o consumidor usou
    para gerar o alerta, então o gráfico comprova a detecção em vez de ilustrá-la.

    Args:
        valores: Média do parâmetro por balde de tempo; None em baldes vazios.
        limite: Valor a partir do qual o consumidor dispara alerta.
        unidade: Unidade exibida nas dicas.
        slot: Índice da cor categórica da linha.
        largura: Largura total do gráfico, em pixels.
        altura: Altura total do gráfico, em pixels.

    Returns:
        O elemento svg completo, ou um aviso se não houver dados.
    """
    pontos = [(i, v) for i, v in enumerate(valores) if v is not None]
    if not pontos:
        return '<p class="vazio">Sem dados para exibir.</p>'

    esq, dir_, topo_m, baixo = 46, 14, 16, 22
    alto = altura - topo_m - baixo
    base = topo_m + alto
    util = largura - esq - dir_
    topo, passo = _escala_agradavel(max(*(v for _, v in pontos), limite) * 1.1)

    def px(i: int) -> float:
        return esq + (i / max(len(valores) - 1, 1)) * util

    def py(v: float) -> float:
        return base - (v / topo) * alto

    caminho = " ".join(
        f"{'M' if k == 0 else 'L'}{px(i):.1f},{py(v):.1f}" for k, (i, v) in enumerate(pontos)
    )
    area = (
        f"M{px(pontos[0][0]):.1f},{base:.1f} "
        + " ".join(f"L{px(i):.1f},{py(v):.1f}" for i, v in pontos)
        + f" L{px(pontos[-1][0]):.1f},{base:.1f} Z"
    )
    fim_i, fim_v = pontos[-1]

    marcas = "".join(
        f'<g class="marca" data-tip="{escape(f"{num(v, 1)} {unidade}")}">'
        f'<circle cx="{px(i):.1f}" cy="{py(v):.1f}" r="7" fill="transparent"/></g>'
        for i, v in pontos
    )

    return (
        f'<svg viewBox="0 0 {largura} {altura}" width="100%" height="{altura}" '
        f'role="img" class="gr">'
        f"{_eixo_y(topo, passo, esq - 8, base, alto)}"
        f'<path d="{area}" fill="var(--serie-{slot})" opacity="0.10"/>'
        f'<path d="{caminho}" fill="none" stroke="var(--serie-{slot})" stroke-width="2" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<line x1="{esq}" y1="{py(limite):.1f}" x2="{largura - dir_}" y2="{py(limite):.1f}" '
        f'class="limite"/>'
        f'<text x="{largura - dir_}" y="{py(limite) - 6:.1f}" text-anchor="end" '
        f'class="rot-limite">limite {num(limite, 1)}</text>'
        f'<circle cx="{px(fim_i):.1f}" cy="{py(fim_v):.1f}" r="4.5" '
        f'fill="var(--serie-{slot})" stroke="var(--surface-1)" stroke-width="2"/>'
        f"{marcas}</svg>"
    )


def legenda(consumidores: list[str]) -> str:
    """Monta a legenda que liga cada cor ao seu consumidor.

    Args:
        consumidores: Nomes dos consumidores, na ordem das cores.

    Returns:
        O bloco HTML da legenda, vazio quando há uma só série.
    """
    if len(consumidores) < MIN_SERIES_LEGENDA:
        return ""
    itens = "".join(
        f'<span class="item"><i style="background:var(--serie-{i % MAX_SERIES + 1})"></i>'
        f"{escape(rotulo_curto(nome))}</span>"
        for i, nome in enumerate(consumidores)
    )
    return f'<div class="legenda">{itens}</div>'


CSS = """
:root{color-scheme:light;
--plano:#f9f9f7;--surface-1:#fcfcfb;--text-primary:#0b0b0b;--text-secondary:#52514e;
--muted:#898781;--grade:#e1e0d9;--base:#c3c2b7;--borda:rgba(11,11,11,.10);
--critico:#d03b3b;--bom:#0ca30c;
--serie-1:#2a78d6;--serie-2:#eb6834;--serie-3:#1baf7a;--serie-4:#eda100;
--serie-5:#e87ba4;--serie-6:#008300;--serie-7:#4a3aa7;--serie-8:#e34948;}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;
--plano:#0d0d0d;--surface-1:#1a1a19;--text-primary:#fff;--text-secondary:#c3c2b7;
--muted:#898781;--grade:#2c2c2a;--base:#383835;--borda:rgba(255,255,255,.10);
--serie-1:#3987e5;--serie-2:#d95926;--serie-3:#199e70;--serie-4:#c98500;
--serie-5:#d55181;--serie-6:#008300;--serie-7:#9085e9;--serie-8:#e66767;}}
:root[data-theme="dark"]{color-scheme:dark;
--plano:#0d0d0d;--surface-1:#1a1a19;--text-primary:#fff;--text-secondary:#c3c2b7;
--muted:#898781;--grade:#2c2c2a;--base:#383835;--borda:rgba(255,255,255,.10);
--serie-1:#3987e5;--serie-2:#d95926;--serie-3:#199e70;--serie-4:#c98500;
--serie-5:#d55181;--serie-6:#008300;--serie-7:#9085e9;--serie-8:#e66767;}
*{box-sizing:border-box}
body{margin:0;padding:24px 16px 64px;background:var(--plano);color:var(--text-primary);
font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;}
.env{max-width:1180px;margin:0 auto}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline;justify-content:space-between;
margin-bottom:6px}
h1{font-size:22px;margin:0;letter-spacing:-.01em}
h2{font-size:15px;margin:32px 0 12px;letter-spacing:-.01em}
h3{font-size:13px;margin:0 0 10px;color:var(--text-secondary);font-weight:600}
.sub{color:var(--muted);font-size:13px;margin:0 0 20px}
.tema{border:1px solid var(--borda);background:var(--surface-1);color:var(--text-secondary);
border-radius:8px;padding:6px 12px;font-size:12px;cursor:pointer;font-family:inherit}
.cartao{background:var(--surface-1);border:1px solid var(--borda);border-radius:12px;
padding:16px 18px;margin-bottom:14px}
.grade-kpi{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
margin-bottom:8px}
.kpi{background:var(--surface-1);border:1px solid var(--borda);border-radius:12px;padding:14px 16px}
.kpi .rotulo{color:var(--text-secondary);font-size:12px;margin-bottom:6px}
.kpi .numero{font-size:27px;font-weight:600;letter-spacing:-.02em}
.kpi .nota{color:var(--muted);font-size:12px;margin-top:2px}
.duas{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(330px,1fr))}
.quatro{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.gr{display:block;overflow:visible}
.gr .grade{stroke:var(--grade);stroke-width:1}
.gr .tick{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.gr .rot{fill:var(--text-secondary);font-size:12px}
.gr .val{fill:var(--text-primary);font-size:12px;font-variant-numeric:tabular-nums}
.gr .limite{stroke:var(--critico);stroke-width:1.5;stroke-dasharray:5 4}
.gr .rot-limite{fill:var(--critico);font-size:10px}
.gr .marca{cursor:default}
.gr .marca:hover{opacity:.78}
.legenda{display:flex;flex-wrap:wrap;gap:14px;margin-top:10px;font-size:12px;
color:var(--text-secondary)}
.legenda .item{display:flex;align-items:center;gap:6px}
.legenda i{width:11px;height:11px;border-radius:3px;display:inline-block}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--borda)}
th{color:var(--text-secondary);font-weight:600;font-size:11.5px;text-transform:uppercase;
letter-spacing:.04em}
.n{text-align:right;font-variant-numeric:tabular-nums}
.celula{text-align:right;font-variant-numeric:tabular-nums;border-radius:5px}
.aviso{border-left:3px solid var(--critico);padding:10px 14px;color:var(--text-secondary);
background:var(--surface-1);border-radius:0 8px 8px 0;font-size:13px}
.vazio{color:var(--muted);font-size:13px;padding:18px 0;margin:0}
.dica{position:fixed;pointer-events:none;opacity:0;transition:opacity .1s;
background:var(--text-primary);color:var(--surface-1);padding:5px 9px;border-radius:6px;
font-size:12px;z-index:9;white-space:nowrap}
footer{color:var(--muted);font-size:12px;margin-top:34px;border-top:1px solid var(--borda);
padding-top:14px}
"""

JS = """
const dica=document.createElement('div');dica.className='dica';document.body.appendChild(dica);
document.addEventListener('mouseover',e=>{const a=e.target.closest('[data-tip]');
if(!a)return;dica.textContent=a.dataset.tip;dica.style.opacity='1';});
document.addEventListener('mousemove',e=>{if(dica.style.opacity==='1'){
dica.style.left=Math.min(e.clientX+14,innerWidth-dica.offsetWidth-8)+'px';
dica.style.top=(e.clientY-34)+'px';}});
document.addEventListener('mouseout',e=>{if(e.target.closest('[data-tip]'))
dica.style.opacity='0';});
document.querySelector('.tema').addEventListener('click',()=>{
const escuro=matchMedia('(prefers-color-scheme: dark)').matches;
const atual=document.documentElement.dataset.tema||(escuro?'dark':'light');
document.documentElement.dataset.theme=atual==='dark'?'light':'dark';
document.documentElement.dataset.tema=atual==='dark'?'light':'dark';});
"""


def bloco_kpis(r: dict) -> str:
    """Monta a faixa de indicadores no topo da página.

    Args:
        r: Resumo devolvido por agregar().

    Returns:
        O bloco HTML com os seis indicadores.
    """
    pct = 100 * r["leituras_com_alerta"] / r["total_leituras"] if r["total_leituras"] else 0
    cartoes = [
        ("Leituras processadas", num(r["total_leituras"]), "gravadas em arquivo"),
        ("Alertas gerados", num(r["total_alertas"]), f"{num(pct, 1)}% das leituras"),
        ("Consumidores", num(len(r["por_consumidor"])), "que gravaram no período"),
        ("Partições ativas", num(len(r["por_particao"])), "do tópico de sensores"),
        ("Vazão média", num(r["vazao"], 1), "mensagens por segundo"),
        ("Duplicatas", num(r["duplicatas"]), "pares (partição, offset)"),
    ]
    return '<div class="grade-kpi">' + "".join(
        f'<div class="kpi"><div class="rotulo">{rot}</div>'
        f'<div class="numero">{valor}</div><div class="nota">{nota}</div></div>'
        for rot, valor, nota in cartoes
    ) + "</div>"


def bloco_matriz(r: dict) -> str:
    """Monta a tabela que cruza consumidores e partições.

    Args:
        r: Resumo devolvido por agregar().

    Returns:
        A tabela HTML, ou um aviso se não houver dados.
    """
    consumidores = sorted({c for c, _ in r["matriz"]})
    particoes = sorted({p for _, p in r["matriz"] if p is not None})
    if not consumidores or not particoes:
        return '<p class="vazio">Sem dados para exibir.</p>'

    maximo = max(r["matriz"].values()) or 1
    cabecalho = "".join(f'<th class="n">Partição {p}</th>' for p in particoes)
    linhas = []
    for consumidor in consumidores:
        celulas = []
        for particao in particoes:
            valor = r["matriz"].get((consumidor, particao), 0)
            intensidade = 0.5 * valor / maximo
            celulas.append(
                f'<td class="celula" style="background:color-mix(in oklab,'
                f"var(--serie-1) {intensidade * 100:.0f}%,var(--surface-1))\">"
                f"{num(valor) if valor else '—'}</td>"
            )
        total = sum(r["matriz"].get((consumidor, p), 0) for p in particoes)
        linhas.append(
            f"<tr><td>{escape(rotulo_curto(consumidor))}</td>"
            f'{"".join(celulas)}<td class="n">{num(total)}</td></tr>'
        )
    return (
        f"<table><thead><tr><th>Consumidor</th>{cabecalho}"
        f'<th class="n">Total</th></tr></thead><tbody>{"".join(linhas)}</tbody></table>'
    )


def bloco_cluster(cluster: dict | None) -> str:
    """Monta o painel com o estado ao vivo do cluster.

    Args:
        cluster: Dados devolvidos por consultar_cluster(), ou None.

    Returns:
        O bloco HTML do painel, ou o aviso de cluster indisponível.
    """
    if not cluster:
        return (
            '<p class="aviso">Cluster fora do ar no momento da geração: esta página foi '
            "montada apenas a partir dos arquivos coletados. Os números acima continuam "
            "válidos; apenas o estado ao vivo (réplicas, HPA e lag) não pôde ser lido.</p>"
        )

    pods = "".join(
        f"<tr><td>{escape(p['nome'])}</td><td>{escape(p['fase'])}</td></tr>"
        for p in cluster["pods"]
    )
    lag = "".join(
        f'<tr><td class="n">{linha["particao"]}</td><td class="n">{linha["offset"]}</td>'
        f'<td class="n">{linha["fim"]}</td><td class="n">{linha["lag"]}</td>'
        f"<td>{escape(rotulo_curto(linha['consumidor']))}</td></tr>"
        for linha in cluster["lag"]
    )
    quadro_lag = (
        f"<table><thead><tr><th>Partição</th><th>Offset</th><th>Fim do log</th>"
        f"<th>Lag</th><th>Consumidor</th></tr></thead><tbody>{lag}</tbody></table>"
        if lag
        else '<p class="vazio">Lag indisponível (broker fora do ar).</p>'
    )
    return (
        f'<div class="grade-kpi">'
        f'<div class="kpi"><div class="rotulo">Réplicas prontas</div>'
        f'<div class="numero">{num(cluster["replicas_prontas"])}</div>'
        f'<div class="nota">de {num(cluster["replicas_desejadas"])} desejadas</div></div>'
        f'<div class="kpi"><div class="rotulo">HPA</div>'
        f'<div class="numero">{num(cluster["hpa_atual"])}</div>'
        f'<div class="nota">entre {num(cluster["hpa_min"])} e {num(cluster["hpa_max"])}'
        f"</div></div>"
        f'<div class="kpi"><div class="rotulo">CPU média</div>'
        f'<div class="numero">{num(cluster["cpu"])}%</div>'
        f'<div class="nota">alvo de 50% para escalar</div></div></div>'
        f'<div class="duas"><div class="cartao"><h3>Pods do consumidor</h3>'
        f"<table><thead><tr><th>Pod</th><th>Estado</th></tr></thead>"
        f"<tbody>{pods}</tbody></table></div>"
        f'<div class="cartao"><h3>Lag por partição</h3>{quadro_lag}</div></div>'
    )


def tabela_alertas(alertas: list[dict]) -> str:
    """Monta a tabela com os alertas mais recentes.

    Args:
        alertas: Registros do arquivo de alertas, do mais recente ao mais antigo.

    Returns:
        A tabela HTML, ou um aviso se não houver alertas.
    """
    if not alertas:
        return '<p class="vazio">Nenhum alerta registrado.</p>'
    linhas = "".join(
        f"<tr><td>{escape(str(a.get('gravado_em', ''))[11:19])}</td>"
        f"<td>{escape(str(a.get('sensor_id', '')))}</td>"
        f"<td>{escape(str(a.get('setor', '')))}</td>"
        f"<td>{escape(str(a.get('parametro', '')))}</td>"
        f'<td class="n">{num(a.get("valor"), 2)}</td>'
        f'<td class="n">{num(a.get("limite"), 1)}</td>'
        f'<td class="n">{a.get("particao", "—")}</td></tr>'
        for a in alertas
    )
    return (
        f"<table><thead><tr><th>Hora</th><th>Sensor</th><th>Setor</th><th>Parâmetro</th>"
        f'<th class="n">Valor</th><th class="n">Limite</th><th class="n">Part.</th>'
        f"</tr></thead><tbody>{linhas}</tbody></table>"
    )


def _secao_balanceamento(r: dict) -> str:
    """Monta a seção de balanceamento de carga.

    Args:
        r: Resumo devolvido por agregar().

    Returns:
        O HTML da seção, correspondente ao objetivo 3 do enunciado.
    """
    # Mesma ordem usada em r["consumidores"], para que cada consumidor mantenha a
    # sua cor entre este gráfico e a linha do tempo da seção 2.
    consumidores = [
        (rotulo_curto(nome), total) for nome, total in r["por_consumidor"].most_common()
    ]
    particoes = [
        (f"Partição {p}", total) for p, total in sorted(r["por_particao"].items(), key=str)
    ]
    return (
        "<h2>1. Balanceamento de carga entre os consumidores</h2>"
        f'<div class="duas">'
        f'<div class="cartao"><h3>Leituras processadas por consumidor</h3>'
        f"{barras_horizontais(consumidores, por_item=True)}</div>"
        f'<div class="cartao"><h3>Leituras por partição</h3>'
        f"{barras_horizontais(particoes, 3)}</div></div>"
        f'<div class="cartao"><h3>Quem leu qual partição</h3>'
        f"{bloco_matriz(r)}</div>"
    )


def _secao_sensores(r: dict) -> str:
    """Monta a seção do mini-mundo, com os sensores e as anomalias.

    Args:
        r: Resumo devolvido por agregar().

    Returns:
        O HTML da seção, com as séries, os alertas e a tabela final.
    """
    por_parametro = [(p, n) for p, n in r["por_parametro"].most_common() if p]
    por_setor = [(s, n) for s, n in r["por_setor"].most_common() if s]

    # Os quatro gráficos usam a mesma cor de propósito: cada um tem série única e
    # título próprio, então a cor não distingue nada. Variar o tom aqui só criaria
    # uma identidade falsa — e colocaria amarelo ao lado de laranja, par que reprova
    # nos limites de daltonismo quando os gráficos são lidos lado a lado.
    graficos = []
    for chave, titulo, unidade, variavel, padrao in PARAMETROS:
        limite = float(os.getenv(variavel, padrao))
        graficos.append(
            f'<div class="cartao"><h3>{titulo} ({unidade}) — média no tempo</h3>'
            f"{serie_com_limite(r['series'][chave], limite, unidade, 1)}</div>"
        )

    return (
        "<h2>3. Mini-mundo: sensores e anomalias</h2>"
        f'<div class="duas">'
        f'<div class="cartao"><h3>Alertas por parâmetro</h3>'
        f"{barras_horizontais(por_parametro, 8)}</div>"
        f'<div class="cartao"><h3>Alertas por setor</h3>'
        f"{barras_horizontais(por_setor, 2)}</div></div>"
        f'<div class="quatro">{"".join(graficos)}</div>'
        f'<div class="cartao"><h3>Últimos alertas registrados</h3>'
        f"{tabela_alertas(r['ultimos_alertas'])}</div>"
    )


def montar_html(r: dict, cluster: dict | None) -> str:
    """Monta o documento HTML completo do dashboard.

    Args:
        r: Resumo devolvido por agregar().
        cluster: Estado do cluster, ou None se ele estiver indisponível.

    Returns:
        O documento HTML inteiro, pronto para ser gravado.
    """
    if r["inicio"] and r["fim"]:
        janela = (
            f'{r["inicio"].astimezone().strftime("%d/%m/%Y %H:%M:%S")} até '
            f'{r["fim"].astimezone().strftime("%H:%M:%S")} '
            f'({num(r["duracao"], 0)} s de coleta)'
        )
    else:
        janela = "sem registros no período"

    gerado = datetime.now(UTC).astimezone().strftime("%d/%m/%Y %H:%M:%S")
    return (
        "<!DOCTYPE html><html lang=\"pt-BR\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>Fábrica Inteligente — Dashboard</title>"
        f"<style>{CSS}</style></head><body><div class=\"env\">"
        "<header><h1>Fábrica Inteligente — monitoramento de sensores</h1>"
        '<button class="tema" type="button">Alternar tema</button></header>'
        f'<p class="sub">Janela dos dados: {janela} · Página gerada em {gerado}</p>'
        f"{bloco_kpis(r)}"
        f"{_secao_balanceamento(r)}"
        "<h2>2. Failover e elasticidade</h2>"
        '<div class="cartao"><h3>Leituras ao longo do tempo, por consumidor</h3>'
        f"{colunas_empilhadas(r['baldes'], r['consumidores'], (r['inicio'], r['fim']))}"
        f"{legenda(r['consumidores'])}</div>"
        f"{bloco_cluster(cluster)}"
        f"{_secao_sensores(r)}"
        "<footer>Gerado por <code>make dashboard</code> a partir dos arquivos JSON Lines "
        "gravados pelos consumidores. A gravação ocorre antes do commit do offset, "
        "portanto a semântica é <em>at-least-once</em>: duplicatas após um rebalanceamento "
        "são esperadas e identificáveis pelo par (partição, offset).</footer>"
        f"</div><script>{JS}</script></body></html>"
    )


def main() -> None:
    """Lê os arquivos, consulta o cluster e grava o dashboard em disco."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(levelname)s - %(message)s",
    )

    pasta = Path(os.getenv("PASTA_DADOS", "dados"))
    leituras = carregar_jsonl(pasta / os.getenv("ARQUIVO_DADOS", "dados-processados.jsonl"))
    alertas = carregar_jsonl(pasta / os.getenv("ARQUIVO_ALERTAS", "alertas.jsonl"))

    if not leituras:
        logger.warning(
            "Nenhuma leitura encontrada. Rode 'make dados' com o cluster no ar "
            "para coletar os arquivos antes de gerar o dashboard."
        )

    resumo = agregar(leituras, alertas)
    cluster = consultar_cluster(os.getenv("NAMESPACE", "fabrinteligente"))

    destino = pasta / os.getenv("ARQUIVO_DASHBOARD", "dashboard.html")
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(montar_html(resumo, cluster), encoding="utf-8")

    logger.info(f"Dashboard gravado em {destino} ({destino.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
