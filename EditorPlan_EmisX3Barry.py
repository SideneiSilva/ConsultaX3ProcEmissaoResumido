import pandas as pd
import os
import re

# Pasta onde ficam as planilhas
pasta = r"S:\Faturamento\RelatoriosX3\EDITAVEL"

# Lista apenas arquivos Excel que começam com "CONSULTA"
arquivos = [f for f in os.listdir(pasta) if f.startswith("CONSULTA") and f.endswith((".xlsx", ".xls"))]

# Encontra o arquivo Grid
arquivos_grid = [f for f in os.listdir(pasta) if f.startswith("Grid") and f.endswith((".xlsx", ".xls"))]

# Dicionário de regras (CNPJ, Cliente) -> Operação
regras_operacao = {
    ("45.575.157/0001-83", "BARRY CALLEBAUT BRASIL INDUSTRIA E COMERCIO DE PRODUTOS ALIMENTICIOS LTDA 33.163.908/0085-83"): "CHOCOLATE PO MATRIZ - SP",
    ("45.575.157/0001-83", "BARRY CALLEBAUT IND. COM. PROD 33.163.908/0105-61"): "CACAU PO MATRIZ - SP",
    ("45.575.157/0002-64", "BARRY CALLEBAUT BRASIL INDUSTRIA E COMERCIO DE PRODUTOS ALIMENTICIOS LTDA 33.163.908/0085-83"): "CHOCOLATE PO FILIAL - MG",
    ("45.575.157/0002-64", "BARRY CALLEBAUT IND. COM. PROD 33.163.908/0105-61"): "CACAU PO FILIAL - MG",
}

# Função para normalizar placa (remover espaços, converter para maiúsculas)
def normalizar_placa(placa):
    if pd.isna(placa):
        return ""
    return str(placa).strip().upper().replace(" ", "").replace("-", "")

# Função para normalizar número (remover espaços, pontos, etc)
def normalizar_numero(numero):
    if pd.isna(numero):
        return ""
    return str(numero).strip().replace(" ", "").replace(".", "").replace("-", "")

# Função para preencher a coluna Apontamento
def definir_apontamento(row):
    operacao = row.get("Operacao", "")
    obs = str(row.get("dc_observacao", "")).upper().strip()

    if operacao == "CHOCOLATE PO MATRIZ - SP":
        if obs.startswith("VG") or obs.startswith("ISENÇÃO"):
            return "VIAGEM MATRIZ SP - CHOCOLATE"
        elif "VG" in obs:
            palavras = obs.split()
            for i, p in enumerate(palavras):
                if p == "VG" and i > 0:
                    return f"{palavras[i-1]} MATRIZ SP - CHOCOLATE"

    elif operacao == "CACAU PO MATRIZ - SP":
        if obs.startswith("VG") or obs.startswith("ISENÇÃO"):
            return "VIAGEM CACAU PO MATRIZ - SP"
        elif "VG" in obs:
            palavras = obs.split()
            for i, p in enumerate(palavras):
                if p == "VG" and i > 0:
                    return f"{palavras[i-1]} CACAU PO MATRIZ - SP"

    elif operacao == "CHOCOLATE PO FILIAL - MG":
        if obs.startswith("ISENÇÃO"):
            return "VIAGEM CHOCOLATE PO FILIAL - MG"
        elif "VG" in obs:
            palavras = obs.split()
            for i, p in enumerate(palavras):
                if p == "VG" and i > 0:
                    return f"{palavras[i-1]} CHOCOLATE PO FILIAL - MG"

    elif operacao == "CACAU PO FILIAL - MG":
        if obs.startswith("ISENÇÃO"):
            return "VIAGEM CACAU PO FILIAL - MG"
        elif "VG" in obs:
            palavras = obs.split()
            for i, p in enumerate(palavras):
                if p == "VG" and i > 0:
                    return f"{palavras[i-1]} CACAU PO FILIAL - MG"

    return "NÃO DEFINIDO"

# Função para validar placa - COM NORMALIZAÇÃO MELHORADA
def validar_placa(row, df_grid):
    if pd.isna(row.get('Viagem')):
        return "Viagem não encontrada na observação"
    
    # Normaliza o número da viagem
    viagem = normalizar_numero(row['Viagem'])
    
    # Normaliza a placa atual
    placa_atual = normalizar_placa(row.get('equipamento_tracao', ''))
    
    if not placa_atual or placa_atual == 'NAN':
        return "Placa não informada no X3"
    
    # Normaliza a coluna Id do Grid para comparação
    df_grid['Id_normalizado'] = df_grid['Id'].apply(normalizar_numero)
    viagem_grid = df_grid[df_grid['Id_normalizado'] == viagem]
    
    if viagem_grid.empty:
        return "Viagem não encontrada no Ravex"
    
    # Normaliza a placa do Grid também
    placa_grid = normalizar_placa(viagem_grid.iloc[0]['Placa'])
    
    if placa_atual == placa_grid:
        return "Placa ok no Ravex"
    else:
        return f"Placa {placa_grid} encontrada no Ravex"

if not arquivos:
    print("Nenhuma planilha encontrada na pasta.")
else:
    # Carrega o arquivo Grid
    df_grid = None
    if arquivos_grid:
        try:
            caminho_grid = os.path.join(pasta, arquivos_grid[0])
            df_grid = pd.read_excel(caminho_grid)
            print(f"=== Arquivo Grid carregado: {arquivos_grid[0]}")  # Alterado para ===
            
            if 'Id' in df_grid.columns:
                df_grid['Id'] = pd.to_numeric(df_grid['Id'], errors='coerce')
                
        except Exception as e:
            print(f"❌ Erro ao carregar Grid: {e}")
            df_grid = None
    else:
        print("⚠ Arquivo Grid não encontrado")
        df_grid = None

    for arquivo in arquivos:
        if arquivo.startswith("Grid"):
            continue
            
        caminho_arquivo = os.path.join(pasta, arquivo)
        print(f"\n=== Editando planilha: {arquivo} ===")

        try:
            df = pd.read_excel(caminho_arquivo)

            if {"nr_cnpj_filial", "nm_cliente", "dc_observacao"}.issubset(df.columns):
                # Etapa 1: Criar coluna Operacao
                df["Operacao"] = df.apply(
                    lambda row: regras_operacao.get(
                        (row["nr_cnpj_filial"], row["nm_cliente"]), "NÃO DEFINIDO"
                    ),
                    axis=1
                )

                # Etapa 2: Criar coluna Apontamento
                df["Apontamento"] = df.apply(definir_apontamento, axis=1)
                
                # Etapa 3: Validar placa (se Grid disponível)
                if df_grid is not None and 'equipamento_tracao' in df.columns:
                    df["Validar Placa"] = df.apply(
                        lambda row: validar_placa(row, df_grid), 
                        axis=1
                    )
                    print("✅ Validação de placa realizada")
                else:
                    df["Validar Placa"] = "Validação não realizada"

                # Etapa 4: Salvar planilha
                df.to_excel(caminho_arquivo, index=False)
                print(f"✅ Planilha atualizada com sucesso: {arquivo}")
                
                # Estatísticas finais - FORMATO AJUSTADO
                viagens_encontradas = df['Viagem'].notna().sum()
                
                if 'Validar Placa' in df.columns:
                    validacoes = df['Validar Placa'].value_counts()
                    
                    # Contadores para estatísticas
                    placas_ok = 0
                    viagens_nao_encontradas_ravex = 0
                    viagens_nao_encontradas_obs = 0
                    placas_nao_informadas = 0
                    placas_divergentes = 0
                    
                    for resultado, quantidade in validacoes.items():
                        if resultado == "Placa ok no Ravex":
                            placas_ok += quantidade
                        elif resultado == "Viagem não encontrada no Ravex":
                            viagens_nao_encontradas_ravex += quantidade
                        elif resultado == "Viagem não encontrada na observação":
                            viagens_nao_encontradas_obs += quantidade
                        elif resultado == "Placa não informada no X3":
                            placas_nao_informadas += quantidade
                        elif "Placa" in resultado and "encontrada no Ravex" in resultado:
                            placas_divergentes += quantidade
                    
                    # Resumo final no formato solicitado
                    print(f"\n📊 Resumo:")
                    print(f" Viagens identificadas: {viagens_encontradas}/{len(df)}")
                    print(f"   Resultados da validação:")
                    print(f"     ✅ Placa ok no Ravex: {placas_ok}")
                    print(f"     ⚠ Viagem não encontrada no Ravex: {viagens_nao_encontradas_ravex}")
                    print(f"     ⚠ Viagem não encontrada na observação: {viagens_nao_encontradas_obs}")
                    print(f"     ⚠ Placas não informadas: {placas_nao_informadas}")
                    print(f"     ❌ Placas divergentes: {placas_divergentes}")
                
            else:
                print("⚠ Colunas necessárias não encontradas na planilha.")
        
        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")

print("\n✅ Processamento concluído!")

# Para alterar o arquivo executável, execute o seguinte comando no prompt de comando: pyinstaller --onefile EditorPlan_EmisX3Barry.py