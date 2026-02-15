import pandas as pd
import os
import platform

SO = platform.system()


# =============================================================================
# 1. CARGA DOS ARQUIVOS
# =============================================================================



# Set paths based on OS
if SO == 'Windows':
    BASE_INPUT  = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\input'
    BASE_OUTPUT = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\output'
else:  # Linux, macOS, etc.
    BASE_INPUT  = r'/home/stark/Documentos/Dev/Necessidade - Slitter/Files/input'
    BASE_OUTPUT = r'/home/stark/Documentos/Dev/Necessidade - Slitter/Files/output'

print(f"Sistema Operacional: {SO}")

#Base windown
#BASE_INPUT  = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\input'
#BASE_OUTPUT = r'C:\Users\jefersson.souza\OneDrive - Açotel Indústria e Comércio LTDA\#PCP\Necessidade - Slitter\Files\output'


#Base linux
#BASE_INPUT  = r'/home/stark/Documentos/Dev/Necessidade - Slitter/Files/input'
#BASE_OUTPUT = r'/home/stark/Documentos/Dev/Necessidade - Slitter/Files/output'

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
# 2. MERGE — trazer 'Data sequenciamento' do CR para cada df_itl
# =============================================================================

pares = [
    (df_itl50_01,  df_CR_itl50_01),
    (df_itl50_02,  df_CR_itl50_02),
    (df_itl75_01,  df_CR_itl75_01),
    (df_itl100_01, df_CR_itl100_01),
    (df_itl130_01, df_CR_itl130_01),
]

dfs_merged = []
for df_itl, df_CR in pares:
    df_merged = df_itl.merge(
        df_CR[['Ordem', 'Data sequenciamento']],
        on='Ordem',
        how='left'
    )
    dfs_merged.append(df_merged)

# =============================================================================
# 3. CONSOLIDAÇÃO
# =============================================================================

df_cronograma = pd.concat(dfs_merged, ignore_index=True)

# =============================================================================
# 4. PREPARAÇÃO DO CRONOGRAMA
# =============================================================================

df_saldo_prod = df_cronograma[[
    'Material', 'Lista comp.item', 'Data sequenciamento',
    'Qtd.necessária (EINHEIT)', 'Texto breve material'
]].copy()

df_saldo_prod['Data sequenciamento'] = pd.to_datetime(df_saldo_prod['Data sequenciamento'], errors='coerce')
df_saldo_prod['Qtd.necessária (EINHEIT)'] = pd.to_numeric(df_saldo_prod['Qtd.necessária (EINHEIT)'], errors='coerce')

# Remover linhas com quantidade <= 0 ou sem data de sequenciamento
df_saldo_prod = df_saldo_prod[df_saldo_prod['Qtd.necessária (EINHEIT)'] > 0]
df_saldo_prod = df_saldo_prod[df_saldo_prod['Data sequenciamento'].notna()].reset_index(drop=True)

# =============================================================================
# 5. PREPARAÇÃO DO ESTOQUE
# =============================================================================

df_zpp001 = pd.read_excel(os.path.join(BASE_INPUT, 'ZPP001-EXPORT.xlsx'))

df_estoque = df_zpp001[[
    'Material', 'Utilização livre', 'Denom.grupo merc.',
    'Matriz de Conformação', 'Espessura Padrão (mm)'
]].copy()

df_estoque['Utilização livre'] = pd.to_numeric(df_estoque['Utilização livre'], errors='coerce').fillna(0)
df_estoque = df_estoque[df_estoque['Denom.grupo merc.'] == 'IN - FITA SLITTER']
df_estoque = df_estoque[df_estoque['Utilização livre'] > 0].reset_index(drop=True)

# =============================================================================
# 6. MERGE — trazer estoque para o cronograma
# =============================================================================

df_necessidade = df_saldo_prod.merge(
    df_estoque[['Material', 'Utilização livre', 'Matriz de Conformação', 'Espessura Padrão (mm)']],
    on='Material',
    how='left'
)

# =============================================================================
# 7. ORDENAÇÃO: Data sequenciamento
# =============================================================================

df_necessidade = df_necessidade.sort_values(
    ['Data sequenciamento']
).reset_index(drop=True)

# =============================================================================
# 8. CÁLCULO FIFO — saldo consumido por material em ordem cronológica
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
# 9. COLUNAS FINAIS
# =============================================================================

df_necessidade = df_necessidade[[
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
# 10. EXPORTAR
# =============================================================================

os.makedirs(BASE_OUTPUT, exist_ok=True)
caminho_saida = os.path.join(BASE_OUTPUT, 'Necessidade - Slitter.xlsx')
df_necessidade.to_excel(caminho_saida, index=False)

print(f"Exportado: {caminho_saida}")
print(f"Total de linhas : {len(df_necessidade)}")
print(f"\nResumo de Status:")
print(df_necessidade['Status'].value_counts().to_string())