import psycopg2
import pandas as pd
from datetime import datetime
import re

# === Solicita filtros ao usuário ===
data_inicio = input("Digite a data de início (DD-MM-AAAA ou DD/MM/AAAA): ")
data_fim = input("Digite a data de fim (DD-MM-AAAA ou DD/MM/AAAA): ")

# === Função para normalizar datas (entrada usuário -> formato PostgreSQL) ===
def normalizar_data(data_str):
    data_str = data_str.replace('/', '-')
    try:
        data_dt = datetime.strptime(data_str, "%d-%m-%Y")
        # Retorna no formato que o PostgreSQL entende
        return data_dt.strftime("%Y-%m-%d")
    except ValueError:
        print("❌ Formato de data inválido. Use DD-MM-AAAA ou DD/MM/AAAA")
        exit()

# === Converte as datas ===
data_inicio_dt = normalizar_data(data_inicio)
data_fim_dt = normalizar_data(data_fim)

params = [data_inicio_dt, data_fim_dt]

# Configuração da conexão
conn = psycopg2.connect(
    host="10.15.1.100",
    port=5432,
    database="smartlog",
    user="consultoria",
    password="consultoria"
)

# Criação de um cursor
cur = conn.cursor()

sql = """
SELECT DISTINCT
  to_char(CAST(processo.dt_processo AS DATE), 'DD-MM-YYYY') as dt_processo,
  cte.nr_cte,
  tipo_negocio.nm_negocio,
  local_ope.nm_negocio as nm_local_op,
  CAST((cliente.nm_fantasia||' '||cliente.nr_cnpj_cpf) AS varchar(100)) as nm_cliente,
  processo_rodoviario.equipamento_tracao_nr_equipamento as equipamento_tracao,
  processo_rodoviario.vr_total,
  
  -- valores rateados
  COALESCE(((comp_valor_frete.vl_comp * (CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_frete,
  COALESCE(((comp_valor_pedagio.vl_comp * (CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_pedagio,
  COALESCE(((comp_valor_imposto.vl_comp * (CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_imposto,
  COALESCE(((comp_valor_outros.vl_comp * (CASE WHEN (processo_rodoviario.vr_total= 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_outros,
  COALESCE(((comp_valor_descarga.vl_comp * (CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_descarga,
  COALESCE(((pernoite.vl_comp * (CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 0
          ELSE processo_rodoviario.vr_total END))/
          CASE WHEN (processo_rodoviario.vr_total = 0 OR processo_rodoviario.vr_total IS NULL) THEN 1 ELSE processo.vr_receber END),0) as vr_pernoite,

  (SELECT MAX(fatura_receber_parcela.faturareceber_id) 
    FROM fatura_receber_parcela 
    JOIN finreceber_parcela 
      ON finreceber_parcela.id = fatura_receber_parcela.finreceber_parcela_id
   WHERE finreceber_parcela.finreceber_id = processo.finreceber_id) as fatura_id,

  filial_cte.nr_cnpj as nr_cnpj_filial,
  processo.dc_observacao

FROM processo
  JOIN filial ON filial.id = processo.filial_id
  JOIN config_processo ON config_processo.id = processo.config_id
  LEFT JOIN processo_rodoviario ON processo_rodoviario.processo_id = processo.id  
  LEFT JOIN view_equipamento ON view_equipamento.nr_equipamento = processo_rodoviario.equipamento_tracao_nr_equipamento
  LEFT JOIN equipamento_negocio ON (equipamento_negocio.equipamento_id = view_equipamento.id) 
     AND ((CAST(processo.dt_processo AS DATE)) >= equipamento_negocio.dt_inicio) 
     AND (((CAST(processo.dt_processo AS DATE)) <= equipamento_negocio.dt_final) OR equipamento_negocio.dt_final IS NULL)
  LEFT JOIN tipo_negocio ON tipo_negocio.id = equipamento_negocio.tipo_negocio_id
  LEFT JOIN tipo_negocio local_ope ON local_ope.id = processo.tipo_negocio_id
  LEFT JOIN pessoa cliente ON cliente.id = processo.pessoa_cliente_id
  LEFT JOIN processo_comp_valor_c comp_valor_frete ON comp_valor_frete.processo_id = processo.id AND comp_valor_frete.comp_valor_id = 1   
  LEFT JOIN processo_comp_valor_c comp_valor_pedagio ON comp_valor_pedagio.processo_id = processo.id AND comp_valor_pedagio.comp_valor_id = 2  
  LEFT JOIN processo_comp_valor_c comp_valor_imposto ON comp_valor_imposto.processo_id = processo.id AND comp_valor_imposto.comp_valor_id = 3 
  LEFT JOIN processo_comp_valor_c comp_valor_outros ON comp_valor_outros.processo_id = processo.id AND comp_valor_outros.comp_valor_id = 4    
  LEFT JOIN processo_comp_valor_c comp_valor_descarga ON comp_valor_descarga.processo_id = processo.id AND comp_valor_descarga.comp_valor_id = 7   
  LEFT JOIN processo_comp_valor_c pernoite ON pernoite.processo_id = processo.id AND pernoite.comp_valor_id = 22
  LEFT JOIN cte ON cte.id = processo.cte_id
  LEFT JOIN filial filial_cte ON filial_cte.id= cte.filial_id

WHERE
  processo.tp_situacao IN ('01','02')
  AND CAST(processo.dt_processo AS DATE) BETWEEN %s AND %s
  AND cliente.nm_fantasia ILIKE '%%BARRY%%'
ORDER BY dt_processo, nr_cte
"""


# === Função para formatar valores no padrão brasileiro ===
def formatar_valor(x):
    try:
        if x is None or str(x).strip() in ["", "nan", "NaT", "0E-20"]:
            return "0,00"
        num = float(x)
        return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

# === Funções para extrair Viagem e Data_Processo ===
def extrair_viagem(texto):
    if texto is None:
        return None
    match = re.search(r"VG\s*[-–]\s*(\d+)", texto)
    return int(match.group(1)) if match else None

def extrair_data(row):
    texto = row['dc_observacao']
    dt_processo = row['dt_processo']
    
    if texto is None:
        # Se não tem observação, usa a data do processo
        return dt_processo.split('-')[0] + '/' + dt_processo.split('-')[1]  # Converte DD-MM-YYYY para DD/MM
    
    match = re.search(r"\*(\d{2}/\d{2})", texto)
    if match:
        return match.group(1)
    else:
        # Se não encontrou o padrão na observação, usa a data do processo
        return dt_processo.split('-')[0] + '/' + dt_processo.split('-')[1]  # Converte DD-MM-YYYY para DD/MM

try:
    print(f"Executando consulta...")
    cur.execute(sql, tuple(params))
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=columns)

    # === Ajusta valores numéricos ===
    colunas_valores = ["vr_total","vr_frete","vr_pedagio","vr_imposto","vr_outros","vr_descarga","vr_pernoite"]
    for col in colunas_valores:
        if col in df.columns:
            df[col] = df[col].apply(formatar_valor)

    # === Extrai Viagem ===
    df["Viagem"] = df["dc_observacao"].apply(extrair_viagem)
    
    # === Extrai Data_Processo (usando apply com axis=1 para acessar múltiplas colunas) ===
    df["Data_Processo"] = df.apply(extrair_data, axis=1)

    # === Define nome do arquivo com período filtrado ===
    nome_arquivo = f"CONSULTA_X3_EMISSAO_{data_inicio.replace('/', '-')}_a_{data_fim.replace('/', '-')}.xlsx"
    caminho_arquivo = fr"S:\Faturamento\RelatoriosX3\{nome_arquivo}"

    # Salva em Excel
    df.to_excel(caminho_arquivo, index=False)

    print(f"✅ Arquivo salvo com sucesso em: {caminho_arquivo}")
    print(f"📊 Total de registros encontrados: {len(df)}")
    
    # Mostra estatísticas das viagens extraídas
    viagens_extraidas = df['Viagem'].notna().sum()
    print(f"🔢 Viagens extraídas: {viagens_extraidas}/{len(df)}")
    
    # Mostra quantas datas foram preenchidas da observação vs dt_processo
    datas_observacao = df['dc_observacao'].apply(lambda x: bool(re.search(r"\*(\d{2}/\d{2})", str(x))) if x else False).sum()
    print(f"📅 Datas extraídas da observação: {datas_observacao}/{len(df)}")

except psycopg2.Error as e:
    print("❌ Erro ao executar a consulta:", e)
    print(f"SQL: {sql}")
    print(f"Parâmetros: {params}")

finally:
    cur.close()
    conn.close()

# Para alterar o arquivo executável, instale o pip install pyinstaller
# Para alterar o arquivo executável, execute o seguinte comando no prompt de comando: pyinstaller --onefile ConsultaX3ProcEmissResumido.py