# FIPEdash 🚗

> Painel de Inteligência Automotiva Brasileira — Tabela FIPE × SSP-SP

Pipeline Python completo que coleta, trata e cruza dados da **Tabela FIPE** com ocorrências de **furtos e roubos de veículos (SSP-SP)**, exportando um dataset tratado que alimenta um dashboard Power BI.

---

## 📊 Dashboard

![FIPEdash Preview](assets/dashboard_preview.png)

Layout **Slate Navy** — sidebar azul-marinho, cards elevados, silhueta de carro, tooltip com foto do veículo.

---

## 🗂 Estrutura do projeto

```
FIPEdash/
├── coleta_fipe.py               # Coleta via API FIPE (parallelum v2)
├── tratamento.py                # Limpeza, cruzamento e export final
├── data/
│   ├── fipe_veiculos.csv        # Saída bruta da coleta (amostra)
│   └── fipe_tratado.csv         # Dataset final para o Power BI
├── powerbi/
│   ├── background_fipedash.png  # Fundo do dashboard (1280×720)
│   └── FIPEdash_SlateNavy_theme.json  # Tema de cores do Power BI
├── requirements.txt
└── README.md
```

---

## ⚙️ Como executar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Coletar dados da FIPE

```bash
# Teste rápido (10 marcas)
python coleta_fipe.py --max-marcas 10

# Coleta completa (~100 marcas)
python coleta_fipe.py
```

> A API tem limite de **500 requisições gratuitas/dia**. Para 1000/dia, crie um token gratuito em [fipe.online](https://fipe.online) e use `--token SEU_TOKEN`.

### 3. Tratar e cruzar com dados SSP-SP

```bash
python tratamento.py
```

Gera `data/fipe_tratado.csv` com 18 colunas prontas para o Power BI.

---

## 📁 Colunas do dataset final

| Coluna | Descrição |
|--------|-----------|
| `marca` | Nome da marca |
| `modelo` | Nome do modelo |
| `ano` | Ano do veículo |
| `combustivel` | Tipo de combustível |
| `segmento` | Popular / Compacto / Médio / Premium / Luxo |
| `valor_fipe` | Valor na Tabela FIPE (R$) |
| `valor_medio_marca` | Valor médio de todos os modelos da marca |
| `rank_valor_marca` | Posição no ranking de valor (1 = mais cara) |
| `total_ocorrencias` | Total de roubos/furtos SSP-SP 2024 |
| `frota_estimada` | Frota estimada por marca |
| `taxa_roubo_por_10k` | Ocorrências por 10.000 veículos da frota |
| `rank_risco` | Posição no ranking de risco (1 = maior risco) |
| `score_seguranca` | Score de segurança 0–100 (100 = mais seguro) |
| `referencia_mes` | Mês de referência da tabela FIPE |
| `coletado_em` | Data de execução do pipeline |

---

## 📊 Power BI — Como aplicar o layout

### 1. Tamanho de página
**Exibir → Tamanho da página → Personalizado**
- Largura: `1280`
- Altura: `720`

### 2. Background
**Formatar página → Papel de parede → Imagem**
- Selecionar `background_fipedash.png`
- Ajuste: `Normal` | Transparência: `0%`

### 3. Tema de cores
**Exibir → Temas → Procurar temas**
- Selecionar `FIPEdash_SlateNavy_theme.json`

### 4. Posicionamento dos visuais
Cada visual: **Formatar visual → Geral → Propriedades → Posição**

| Visual | X | Y | Largura | Altura |
|--------|---|---|---------|--------|
| KPI Marcas | 228 | 88 | 252 | 84 |
| KPI Maior Valor | 490 | 88 | 252 | 84 |
| KPI Ocorrências | 752 | 88 | 252 | 84 |
| KPI Taxa de Risco | 1014 | 88 | 252 | 84 |
| Barras (ranking FIPE) | 228 | 182 | 394 | 258 |
| Linha (tendência) | 632 | 182 | 394 | 258 |
| Risco SSP-SP | 1036 | 182 | 232 | 526 |
| Donut (segmentos) | 228 | 450 | 798 | 258 |

---

## 🔧 Stack

| Camada | Tecnologia |
|--------|-----------|
| Coleta | Python · `requests` · API FIPE (parallelum v2) |
| Tratamento | Python · `pandas` |
| Dados de segurança | SSP-SP (dados públicos) |
| Visualização | Microsoft Power BI Desktop |
| Versionamento | Git · GitHub |

---

## 📌 Fonte dos dados

- **Tabela FIPE**: [parallelum.com.br/fipe/api/v2](https://parallelum.com.br/fipe/api/v2) — API pública não oficial, gratuita
- **SSP-SP**: [ssp.sp.gov.br](https://www.ssp.sp.gov.br) — Dados públicos de ocorrências policiais

---

## 👤 Autor

Desenvolvido como projeto de portfólio para demonstração de habilidades em Python (pandas, APIs REST) e Power BI.
