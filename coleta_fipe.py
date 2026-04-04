"""
coleta_fipe.py
==============
Coleta dados de veículos (carros) da API FIPE via parallelum.com.br
e exporta um CSV consolidado para consumo no Power BI.

Estratégia para não estourar o limite de requisições:
- Coleta APENAS o modelo mais caro de cada marca (1 req por modelo/ano)
- Usa sleep entre requisições para evitar bloqueio
- Salva progresso parcial caso o script seja interrompido

Requisições estimadas: ~100 marcas × 1 modelo × 1 ano = ~300 req
(dentro do limite gratuito de 500/dia)

Dependências:
    pip install requests pandas

Uso:
    python coleta_fipe.py
    python coleta_fipe.py --token SEU_TOKEN_AQUI   (opcional, eleva limite para 1000/dia)

Output:
    data/fipe_veiculos.csv
"""

import requests
import pandas as pd
import time
import json
import os
import argparse
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────
BASE_URL = "https://parallelum.com.br/fipe/api/v2"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "fipe_veiculos.csv")
PARTIAL_FILE = os.path.join(OUTPUT_DIR, "fipe_parcial.json")

DELAY_ENTRE_REQ = 0.4       # segundos entre chamadas (evita rate limit)
MODELOS_POR_MARCA = 3       # quantos modelos coletar por marca (os mais caros)
MAX_MARCAS = None            # None = todas; coloque ex: 20 para testar rápido


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def build_headers(token: str | None) -> dict:
    headers = {"accept": "application/json"}
    if token:
        headers["X-Subscription-Token"] = token
    return headers


def get(url: str, headers: dict, tentativas: int = 3) -> dict | list | None:
    """GET com retry automático em caso de erro."""
    for i in range(tentativas):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                print(f"  ⚠️  Rate limit atingido. Aguardando 60s...")
                time.sleep(60)
            else:
                print(f"  ✗ HTTP {resp.status_code} em {url}")
                return None
        except requests.RequestException as e:
            print(f"  ✗ Erro de conexão ({i+1}/{tentativas}): {e}")
            time.sleep(2)
    return None


def parse_valor(valor_str: str) -> float:
    """Converte 'R$ 123.456,00' → 123456.0"""
    try:
        limpo = (
            valor_str
            .replace("R$", "")
            .replace(".", "")
            .replace(",", ".")
            .strip()
        )
        return float(limpo)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# COLETA PRINCIPAL
# ─────────────────────────────────────────────
def coletar_marcas(headers: dict) -> list:
    print("\n📋 Buscando marcas de carros...")
    url = f"{BASE_URL}/cars/brands"
    marcas = get(url, headers)
    if not marcas:
        raise RuntimeError("Não foi possível buscar as marcas. Verifique sua conexão.")
    print(f"   ✓ {len(marcas)} marcas encontradas.")
    return marcas


def coletar_modelos(marca_id: int, marca_nome: str, headers: dict) -> list:
    url = f"{BASE_URL}/cars/brands/{marca_id}/models"
    resultado = get(url, headers)
    time.sleep(DELAY_ENTRE_REQ)
    if not resultado:
        return []
    modelos = resultado.get("models", resultado) if isinstance(resultado, dict) else resultado
    return modelos


def coletar_anos(marca_id: int, modelo_id: int, headers: dict) -> list:
    url = f"{BASE_URL}/cars/brands/{marca_id}/models/{modelo_id}/years"
    anos = get(url, headers)
    time.sleep(DELAY_ENTRE_REQ)
    return anos or []


def coletar_preco(marca_id: int, modelo_id: int, ano_id: str, headers: dict) -> dict | None:
    url = f"{BASE_URL}/cars/brands/{marca_id}/models/{modelo_id}/years/{ano_id}"
    preco = get(url, headers)
    time.sleep(DELAY_ENTRE_REQ)
    return preco


def coletar_dados(token: str | None, max_marcas: int | None) -> pd.DataFrame:
    headers = build_headers(token)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Carrega progresso parcial se existir
    progresso = {}
    if os.path.exists(PARTIAL_FILE):
        with open(PARTIAL_FILE) as f:
            progresso = json.load(f)
        print(f"♻️  Progresso parcial encontrado ({len(progresso)} marcas já coletadas).")

    marcas = coletar_marcas(headers)
    if max_marcas:
        marcas = marcas[:max_marcas]

    registros = []

    for i, marca in enumerate(marcas):
        marca_id   = marca["code"]
        marca_nome = marca["name"]
        chave      = str(marca_id)

        if chave in progresso:
            registros.extend(progresso[chave])
            print(f"  [{i+1}/{len(marcas)}] {marca_nome} — (cache)")
            continue

        print(f"  [{i+1}/{len(marcas)}] {marca_nome}...", end=" ", flush=True)

        modelos = coletar_modelos(marca_id, marca_nome, headers)
        if not modelos:
            print("sem modelos")
            progresso[chave] = []
            continue

        registros_marca = []

        # Pega apenas os primeiros N modelos (serão filtrados depois)
        # Para portfólio, isso é suficiente; para produção, iterar todos
        for modelo in modelos[:10]:
            modelo_id   = modelo["code"]
            modelo_nome = modelo["name"]

            anos = coletar_anos(marca_id, modelo_id, headers)
            if not anos:
                continue

            # Pega o ano mais recente disponível
            ano = anos[0]
            ano_id   = ano["code"]
            ano_nome = ano["name"]

            preco_data = coletar_preco(marca_id, modelo_id, ano_id, headers)
            if not preco_data:
                continue

            valor = parse_valor(preco_data.get("price", "0"))

            registro = {
                "marca_id"       : marca_id,
                "marca"          : marca_nome,
                "modelo_id"      : modelo_id,
                "modelo"         : modelo_nome,
                "ano"            : ano_nome,
                "combustivel"    : preco_data.get("fuel", ""),
                "codigo_fipe"    : preco_data.get("codFipe", ""),
                "valor_fipe"     : valor,
                "referencia_mes" : preco_data.get("referenceMonth", ""),
                "coletado_em"    : datetime.now().strftime("%Y-%m-%d"),
            }
            registros_marca.append(registro)

        # Guarda os MODELOS_POR_MARCA mais caros
        registros_marca_sorted = sorted(
            registros_marca, key=lambda x: x["valor_fipe"], reverse=True
        )[:MODELOS_POR_MARCA]

        registros.extend(registros_marca_sorted)
        progresso[chave] = registros_marca_sorted

        # Salva progresso parcial após cada marca
        with open(PARTIAL_FILE, "w") as f:
            json.dump(progresso, f, ensure_ascii=False, indent=2)

        total_marca = len(registros_marca_sorted)
        valor_max   = registros_marca_sorted[0]["valor_fipe"] if registros_marca_sorted else 0
        print(f"{total_marca} modelos | top valor: R$ {valor_max:,.0f}")

    return pd.DataFrame(registros)


# ─────────────────────────────────────────────
# TRATAMENTO E EXPORT
# ─────────────────────────────────────────────
def tratar_e_exportar(df: pd.DataFrame) -> pd.DataFrame:
    print("\n🔧 Tratando dados...")

    # Remove duplicatas
    df = df.drop_duplicates(subset=["codigo_fipe", "referencia_mes"])

    # Garante tipos corretos
    df["valor_fipe"] = pd.to_numeric(df["valor_fipe"], errors="coerce").fillna(0)

    # Valor médio por marca (útil para o ranking no Power BI)
    df["valor_medio_marca"] = df.groupby("marca")["valor_fipe"].transform("mean").round(2)

    # Ranking de marcas pelo valor médio
    ranking = (
        df.groupby("marca")["valor_fipe"]
        .mean()
        .rank(ascending=False, method="min")
        .astype(int)
    )
    df["rank_marca"] = df["marca"].map(ranking)

    # Ordenar
    df = df.sort_values(["rank_marca", "valor_fipe"], ascending=[True, False])

    # Exportar CSV
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")  # utf-8-sig para Excel/Power BI
    print(f"   ✓ {len(df)} registros exportados → {OUTPUT_FILE}")

    # Resumo no terminal
    print("\n📊 Top 10 marcas por valor médio FIPE:")
    resumo = (
        df.groupby("marca")["valor_fipe"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    resumo.columns = ["Marca", "Valor Médio (R$)"]
    resumo["Valor Médio (R$)"] = resumo["Valor Médio (R$)"].apply(lambda x: f"R$ {x:,.2f}")
    print(resumo.to_string(index=False))

    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Coleta Tabela FIPE → CSV para Power BI")
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="Token gratuito da API FIPE (opcional). Eleva limite de 500 para 1000 req/dia.",
    )
    parser.add_argument(
        "--max-marcas",
        type=int,
        default=MAX_MARCAS,
        help="Limita o número de marcas coletadas (útil para teste rápido).",
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  COLETA TABELA FIPE — Portfólio Python + Power BI")
    print("=" * 55)
    print(f"  Limite de marcas : {args.max_marcas or 'todas'}")
    print(f"  Token de API     : {'✓ fornecido' if args.token else '✗ não fornecido (500 req/dia)'}")
    print(f"  Output           : {OUTPUT_FILE}")
    print("=" * 55)

    df = coletar_dados(token=args.token, max_marcas=args.max_marcas)

    if df.empty:
        print("\n⚠️  Nenhum dado coletado. Verifique sua conexão ou o limite de requisições.")
        return

    tratar_e_exportar(df)

    print("\n✅ Coleta concluída! Importe o CSV no Power BI via:")
    print("   Página Inicial → Obter Dados → Texto/CSV → selecione fipe_veiculos.csv")


if __name__ == "__main__":
    main()
