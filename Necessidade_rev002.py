import pandas as pd
import os
import platform

SO = platform.system()

# =============================================================================
# 1. CARGA DOS ARQUIVOS
# =============================================================================

if SO == 'Windows':
    BASE_INPUT  = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\input'
    BASE_OUTPUT = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\output'
else:
    BASE_INPUT  = r'/home/stark/Documentos/Dev/Necessidade_Slitter_py/Files/input/'
    BASE_OUTPUT = r'/home/stark/Documentos/Dev/Necessidade_Slitter_py/Files/output'

print(f"Sistema Operacional: {SO}")

df_CR_itl50_01  = pd.read_excel(os.path.join(BASE_INPUT, 'CR-ITL50-1-EXPORT.xlsx'))
df_CR_itl50_02  = pd.read_excel(os.path.join(BASE_INPUT, 'CR-ITL50-2-EXPORT.xlsx'))
df_CR_itl75_01  = pd.read_excel(os.path.join(BASE_INPUT, 'CR-ITL75-1-EXPORT.xlsx'))
df_CR_itl100_01 = pd.read_excel(os.path.join(BASE_INPUT, 'CR-ITL100-1-EXPORT.xlsx'))
df_CR_itl130_01 = pd.read_excel(os.path.join(BASE_INPUT, 'CR-ITL130-1-EXPORT.xlsx'))

df_itl50_01  = pd.read_excel(os.path.join(BASE_INPUT, 'ITENS-ITL50-1-EXPORT.xlsx'))
df_itl50_02  = pd.read_excel(os.path.join(BASE_INPUT, 'ITENS-ITL50-2-EXPORT.xlsx'))
df_itl75_01  = pd.read_excel(os.path.join(BASE_INPUT, 'ITENS-ITL75-1-EXPORT.xlsx'))
df_itl100_01 = pd.read_excel(os.path.join(BASE_INPUT, 'ITENS-ITL100-1-EXPORT.xlsx'))
df_itl130_01 = pd.read_excel(os.path.join(BASE_INPUT, 'ITENS-ITL130-1-EXPORT.xlsx'))

# =============================================================================
# 2. PREPARAÇÃO DO CRONOGRAMA
# =============================================================================

df_cronograma_grup = pd.concat(
    [df_CR_itl50_01, df_CR_itl50_02, df_CR_itl75_01, df_CR_itl100_01, df_CR_itl130_01],
    ignore_index=True
)
df_itens_cronograma_grup = pd.concat(
    [df_itl50_01, df_itl50_02, df_itl75_01, df_itl100_01, df_itl130_01],
    ignore_index=True
)

df_itens_cronograma = df_itens_cronograma_grup[df_itens_cronograma_grup['Qtd.necessária (EINHEIT)'] >= 1]
df_itens_cronograma = (
    df_itens_cronograma
    .groupby(['Ordem', 'Material', 'Texto breve material'])['Qtd.necessária (EINHEIT)']
    .sum()
    .reset_index()
)

# =============================================================================
# 3. MERGE PARA TRAZER 'Data sequenciamento' PARA OS ITENS
# =============================================================================

df_datas_ordens = (
    df_cronograma_grup
    .groupby('Ordem')['Data sequenciamento']
    .min()          # data mais cedo da ordem (critério FIFO)
    .reset_index()
)

df_saldo_prod = df_itens_cronograma.merge(df_datas_ordens, on='Ordem', how='left')

# =============================================================================
# 4. PREPARAÇÃO DO ESTOQUE
# =============================================================================

df_zpp001 = pd.read_excel(os.path.join(BASE_INPUT, 'ZPP001-EXPORT.xlsx'))

df_estoque = df_zpp001[[
    'Material', 'Utilização livre', 'Denom.grupo merc.',
    'Matriz de Conformação', 'Espessura Padrão (mm)'
]].copy()

df_estoque['Utilização livre'] = pd.to_numeric(df_estoque['Utilização livre'], errors='coerce').fillna(0)

df_estoque = df_estoque[df_estoque['Denom.grupo merc.'] == 'IN - FITA SLITTER'].reset_index(drop=True)

# =============================================================================
# 5. MERGE — trazer estoque para o cronograma
# =============================================================================

df_necessidade = df_saldo_prod.merge(
    df_estoque[['Material', 'Utilização livre', 'Matriz de Conformação', 'Espessura Padrão (mm)']],
    on='Material',
    how='left'
)

# Preenche NaN de estoque com 0 para materiais sem cadastro ou sem saldo
df_necessidade['Utilização livre'] = df_necessidade['Utilização livre'].fillna(0)

# =============================================================================
# 6. ORDENAÇÃO: Data sequenciamento (FIFO)
# =============================================================================

df_necessidade = df_necessidade.sort_values('Data sequenciamento').reset_index(drop=True)

# =============================================================================
# 7. CÁLCULO FIFO — saldo consumido por material em ordem cronológica
# =============================================================================

df_necessidade['Demanda Acumulada'] = (
    df_necessidade.groupby('Material')['Qtd.necessária (EINHEIT)'].cumsum()
)

df_necessidade['Saldo Projetado'] = (
    df_necessidade['Utilização livre'] - df_necessidade['Demanda Acumulada']
)

df_necessidade['Status'] = df_necessidade['Saldo Projetado'].apply(
    lambda x: 'Atende' if x >= 0 else 'Não Atende'
)

# =============================================================================
# 8. COLUNAS FINAIS
# =============================================================================

df_necessidade = df_necessidade[[
    'Ordem',
    'Material',
    'Texto breve material',
    'Espessura Padrão (mm)',
    'Matriz de Conformação',
    'Data sequenciamento',
    'Qtd.necessária (EINHEIT)',
    'Utilização livre',
    'Demanda Acumulada',
    'Saldo Projetado',
    'Status'
]]

# =============================================================================
# 9. EXPORTAR
# =============================================================================

os.makedirs(BASE_OUTPUT, exist_ok=True)
caminho_saida = os.path.join(BASE_OUTPUT, 'Necessidade - Slitter.xlsx')
df_necessidade.to_excel(caminho_saida, index=False)

print(f"Exportado: {caminho_saida}")
print(f"Total de linhas: {len(df_necessidade)}")
print(f"\nResumo de Status:")
print(df_necessidade['Status'].value_counts().to_string())