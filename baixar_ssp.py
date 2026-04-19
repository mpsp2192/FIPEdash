"""
baixar_ssp.py
=============
Baixa os arquivos de veículos subtraídos da SSP-SP (2017 até o ano atual),
normaliza as colunas removendo as indesejadas e salva a pasta pronta
para ser consumida direto pelo Power BI.

Destino: C:\\Users\\mpsp9\\OneDrive\\Área de Trabalho\\FIPEdash\\data\\Veículos Roubados\\

Regras de normalização:
  - REMOVE:  VERSAO, LOGRADOURO_VERSAO, DESC_NATUREZA_LOCAL
  - REMOVE:  colunas duplicadas (CIDADE pode aparecer 2x em alguns anos)
  - RENOMEIA: MES_REGISTRO_BO → MES, ANO_REGISTRO_BO → ANO, DESCR_COR_VEICULO → DESC_COR_VEICULO
  - PRESERVA: colunas entre FLAG_STATUS e RUBRICA com seus nomes originais

Uso:
    python baixar_ssp.py          # baixa anos que faltam
    python baixar_ssp.py --force  # re-baixa o ano atual (rodar dia 1 de cada mês)

Agendamento Windows — Task Scheduler, mensal dia 1, 08:00:
    Programa  : python
    Argumentos: C:\\Users\\mpsp9\\OneDrive\\Área de Trabalho\\FIPEdash\\baixar_ssp.py --force
"""

import requests
import pandas as pd
import openpyxl
import os
import time
import argparse
from datetime import datetime


PASTA_DESTINO = r"C:\Users\mpsp9\OneDrive\Área de Trabalho\FIPEdash\data\Veículos Roubados"
PASTA_TEMP    = r"C:\Users\mpsp9\OneDrive\Área de Trabalho\FIPEdash\data\temp_ssp"
BASE_URL      = "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/baseDados/veiculosSub"
HEADERS       = {
    "User-Agent": "Mozilla/5.0",
    "Referer"   : "https://www.ssp.sp.gov.br/estatistica/consultas",
}
ANO_ATUAL = datetime.now().year
ANOS      = list(range(2017, ANO_ATUAL + 1))

REMOVER  = {"VERSAO", "LOGRADOURO_VERSAO", "DESC_NATUREZA_LOCAL"}
RENOMEAR = {
    "MES_REGISTRO_BO"  : "MES",
    "ANO_REGISTRO_BO"  : "ANO",
    "DESCR_COR_VEICULO": "DESC_COR_VEICULO",
}


def baixar(ano, forcar=False):
    """Baixa o XLSX bruto na pasta temporária."""
    os.makedirs(PASTA_TEMP, exist_ok=True)
    destino = os.path.join(PASTA_TEMP, f"ssp_raw_{ano}.xlsx")

    if os.path.exists(destino) and not forcar:
        size = os.path.getsize(destino) / 1024 / 1024
        print(f"  ✓ {ano} — cache temporário ({size:.1f} MB)")
        return destino

    if os.path.exists(destino):
        os.remove(destino)

    url = f"{BASE_URL}/VeiculosSubtraidos_{ano}.xlsx"
    print(f"  ↓ {ano}: baixando...", flush=True)

    try:
        r = requests.get(url, headers=HEADERS, timeout=180, stream=True)
        if r.status_code == 404:
            print(f"  ✗ {ano}: não disponível (404)")
            return None
        r.raise_for_status()

        total   = int(r.headers.get("content-length", 0))
        baixado = 0
        with open(destino, "wb") as f:
            for chunk in r.iter_content(512 * 1024):
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    print(f"\r  ↓ {ano}: {baixado/1024/1024:.1f}/{total/1024/1024:.1f} MB",
                          end="", flush=True)
        print()
        size = os.path.getsize(destino) / 1024 / 1024
        print(f"  ✓ {ano}: {size:.1f} MB baixado")
        time.sleep(1)
        return destino

    except Exception as e:
        print(f"  ✗ {ano}: ERRO — {e}")
        if os.path.exists(destino):
            os.remove(destino)
        return None


def normalizar(path_xlsx, ano):
    """Remove colunas indesejadas e duplicadas, salva XLSX normalizado no destino."""

    # Descobre a aba correta
    wb = openpyxl.load_workbook(path_xlsx, read_only=True, data_only=True)
    aba = next((s for s in wb.sheetnames if "VEICULO" in s.upper()), wb.sheetnames[0])
    wb.close()

    print(f"  📖 {ano}: lendo aba '{aba}'...")
    df = pd.read_excel(path_xlsx, sheet_name=aba, dtype=str, engine="openpyxl")

    # Remove colunas None/vazias
    df = df[[c for c in df.columns if c is not None and str(c).strip() not in ("None","")]]

    # Renomeia variações para o padrão
    df = df.rename(columns=RENOMEAR)

    # Remove colunas indesejadas
    df = df.drop(columns=[c for c in df.columns if c in REMOVER], errors="ignore")

    # Remove colunas duplicadas (ex: CIDADE 2x) — mantém primeira ocorrência
    cols_unicas = []
    vistas = set()
    for c in df.columns:
        if c not in vistas:
            cols_unicas.append(c)
            vistas.add(c)
    df = df[cols_unicas]

    os.makedirs(PASTA_DESTINO, exist_ok=True)
    destino = os.path.join(PASTA_DESTINO, f"VeiculosSubtraidos_{ano}.xlsx")

    print(f"  💾 {ano}: salvando {len(df):,} registros × {len(df.columns)} colunas...")
    df.to_excel(destino, index=False, engine="openpyxl", sheet_name=f"VEICULOS_{ano}")
    size = os.path.getsize(destino) / 1024 / 1024
    print(f"  ✓ {ano}: {destino} ({size:.1f} MB)")
    return destino


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Força re-download do ano atual")
    args = parser.parse_args()

    print("=" * 55)
    print(f"  SSP-SP Veículos Subtraídos — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"  Destino: {PASTA_DESTINO}")
    print("=" * 55)

    sucessos = 0
    for ano in ANOS:
        print(f"\n── ANO {ano} ──")
        forcar = args.force and (ano == ANO_ATUAL)
        path_bruto = baixar(ano, forcar=forcar)
        if not path_bruto:
            continue
        try:
            normalizar(path_bruto, ano)
            sucessos += 1
        except Exception as e:
            print(f"  ✗ {ano}: erro na normalização — {e}")

    print()
    print("=" * 55)
    print(f"  ✅ {sucessos} arquivos em {PASTA_DESTINO}")
    print("=" * 55)
    print()
    print("  No Power BI:")
    print(f"    Obter Dados → Pasta → {PASTA_DESTINO}")
    print(f"    Combinar e transformar → tabela única")


if __name__ == "__main__":
    main()
