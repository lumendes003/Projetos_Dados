from pathlib import Path
from dateutil.parser import parse
import pandas as pd
from datetime import datetime
import time

PASTA_BASE = Path('caminho do arquivo')
ARQ_ENTRADA = PASTA_BASE / 'BASE ENTRADA COBERTURA RESERVAS.xlsm'
ARQ_SAIDA   = PASTA_BASE / 'COBERTURA OBRAS RESERVAS ATUAL.xlsx'

def executar():

    start_time = time.time()
    print(f"\nIniciando execução em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    df_plano       = pd.read_excel(ARQ_ENTRADA, sheet_name='PLANO 2025',  header=2)
    df_estoque     = pd.read_excel(ARQ_ENTRADA, sheet_name='Estoque',     header=2)
    df_pedidos     = pd.read_excel(ARQ_ENTRADA, sheet_name='PEDIDOS',     header=2)
    df_requisicoes = pd.read_excel(ARQ_ENTRADA, sheet_name='REQUISIÇÕES', header=0)

    try:
        df_estoque_q = pd.read_excel(ARQ_ENTRADA, sheet_name='Estoque Q', header=2)
    except:
        df_estoque_q = pd.DataFrame(columns=['ELEMENTO PEP', 'chave', 'mês atual Q'])

    try:
        df_pedidos_q = pd.read_excel(ARQ_ENTRADA, sheet_name='PEDIDOS Q', header=2)
    except:
        df_pedidos_q = pd.DataFrame(columns=['ELEMENTO PEP', 'chave'])

    try:
        df_requisicoes_q = pd.read_excel(ARQ_ENTRADA, sheet_name='REQUISIÇÕES Q', header=0)
    except:
        df_requisicoes_q = pd.DataFrame(columns=['ELEMENTO PEP', 'chave', 'TOTAL Q'])

    for df in [df_plano, df_estoque, df_pedidos, df_requisicoes]:
        if 'chave' not in df.columns:
            if 'COD SAP' in df.columns and 'EMPRESA' in df.columns:
                df['chave'] = df['COD SAP'].astype(str).str.strip() + df['EMPRESA'].astype(str).str.strip()
            elif 'COD' in df.columns and 'EMPRESA' in df.columns:
                df['chave'] = df['COD'].astype(str).str.strip() + df['EMPRESA'].astype(str).str.strip()
            else:
                raise ValueError("Não foi possível criar 'chave'.")
        else:
            df['chave'] = df['chave'].astype(str).str.strip()

    for df in [df_plano, df_estoque_q, df_pedidos_q, df_requisicoes_q]:
        if 'ELEMENTO PEP' in df.columns:
            df['ELEMENTO PEP'] = df['ELEMENTO PEP'].astype(str).str.upper().str.strip()

    df_plano['CAT.']             = df_plano['CAT.'].astype(str).str.strip().str.upper()
    df_plano['Nova demanda']     = pd.to_numeric(df_plano['Nova demanda'], errors='coerce').fillna(0.0)
    df_plano['DATA NECESSIDADE'] = pd.to_datetime(df_plano['DATA NECESSIDADE'], errors='coerce')
    df_plano['MES_NEC']          = df_plano['DATA NECESSIDADE'].dt.to_period('M').dt.to_timestamp()

    mask_q    = df_plano['CAT.'] == 'Q'
    mask_norm = ~mask_q

    df_plano['chave_final'] = df_plano.apply(
        lambda x: f"{x['ELEMENTO PEP']}_{x['chave']}" if x['CAT.'] == 'Q' else x['chave'],
        axis=1
    )

    for df in [df_estoque_q, df_pedidos_q, df_requisicoes_q]:
        if 'ELEMENTO PEP' in df.columns and 'chave' in df.columns:
            df['chave_final'] = df['ELEMENTO PEP'] + "_" + df['chave'].astype(str)
        elif 'ELEMENTO PEP' in df.columns:
            df['chave_final'] = df['ELEMENTO PEP']
        else:
            df['chave_final'] = df['chave']

    def normalizar_meses(df):
        if df.empty:
            return df, []
        novas = []
        for col in df.columns:
            try:
                parsed = parse(str(col))
                novas.append(pd.Timestamp(parsed.replace(day=1)))
            except:
                novas.append(col)
        df.columns = novas
        meses = sorted({c for c in df.columns if isinstance(c, pd.Timestamp)})
        for m in meses:
            df[m] = (
                pd.to_numeric(
                    df[m].astype(str)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.extract(r'(\d+\.?\d*)')[0],
                    errors='coerce'
                ).fillna(0.0)
            )
        return df, meses

    df_pedidos,   meses_N = normalizar_meses(df_pedidos)
    df_pedidos_q, meses_Q = normalizar_meses(df_pedidos_q)

    COLS_N = set()
    COLS_Q = set()

    df_q = df_plano[mask_q].copy()

    if not df_q.empty:

        df_q['Estoque Q'] = 0.0

        estQ = dict(zip(
            df_estoque_q['chave_final'],
            pd.to_numeric(df_estoque_q.get('mês atual Q', 0), errors='coerce').fillna(0.0)
        ))

        usado = {}

        for i, row in df_q.iterrows():
            key = row['chave_final']
            dem = row['Nova demanda']
            disp_total = estQ.get(key, 0.0)
            ja = usado.get(key, 0.0)
            saldo = max(disp_total - ja, 0.0)
            alocar = min(dem, saldo)
            df_q.at[i, 'Estoque Q'] = alocar
            usado[key] = ja + alocar

        df_q['DIF_Q'] = (df_q['Nova demanda'] - df_q['Estoque Q']).clip(lower=0.0)

        if meses_Q:
            lista_ped_q = []
            for _, r in df_pedidos_q.iterrows():
                key = r['chave_final']
                for m in meses_Q:
                    qtd = float(r.get(m, 0.0) or 0.0)
                    if qtd > 0:
                        lista_ped_q.append((key, m, qtd))

            df_q['_o'] = range(len(df_q))
            df_q = df_q.sort_values(['chave_final', 'MES_NEC', '_o'])

            for key, mes, qtd_disp in lista_ped_q:
                elig = (df_q['chave_final'] == key) & (df_q['DIF_Q'] > 0)
                for idx in df_q[elig].index:
                    if qtd_disp <= 0:
                        break
                    need = df_q.at[idx, 'DIF_Q']
                    mes_nec = df_q.at[idx, 'MES_NEC']
                    destino = pd.Timestamp(max(mes, mes_nec))
                    if destino not in df_q.columns:
                        df_q[destino] = 0.0
                    COLS_Q.add(destino)
                    alocar = min(need, qtd_disp)
                    df_q.at[idx, destino] += alocar
                    df_q.at[idx, 'DIF_Q'] -= alocar
                    qtd_disp -= alocar

            df_q = df_q.sort_values('_o').drop(columns=['_o'])

        df_q['REQUISIÇÕES Q'] = 0.0

        reqQ = dict(zip(
            df_requisicoes_q['chave_final'],
            pd.to_numeric(df_requisicoes_q.get('TOTAL Q', 0), errors='coerce').fillna(0.0)
        ))

        usado_req = {}
        meses_lista = sorted(COLS_Q)

        for i, row in df_q.iterrows():
            key = row['chave_final']
            soma_ped = row[meses_lista].sum() if meses_lista else 0.0
            falta = row['Nova demanda'] - row['Estoque Q'] - soma_ped
            if falta <= 0:
                continue
            disp_total = reqQ.get(key, 0.0)
            ja = usado_req.get(key, 0.0)
            saldo = max(disp_total - ja, 0.0)
            alocar = min(falta, saldo)
            df_q.at[i, 'REQUISIÇÕES Q'] = alocar
            usado_req[key] = ja + alocar

        soma_ped = df_q[meses_lista].sum(axis=1) if meses_lista else 0.0
        df_q['DESCOBERTO Q'] = (
            df_q['Nova demanda']
            - df_q['Estoque Q']
            - df_q['REQUISIÇÕES Q']
            - soma_ped
        ).clip(lower=0.0)

    df_n = df_plano[mask_norm].copy()

    if not df_n.empty:

        df_n['Estoque'] = 0.0
        estN = dict(zip(
            df_estoque['chave'],
            pd.to_numeric(df_estoque.get('mês atual', 0), errors='coerce')
        ))

        usado = {}

        for i, row in df_n.iterrows():
            key = row['chave']
            dem = row['Nova demanda']
            disp_total = estN.get(key, 0.0)
            ja = usado.get(key, 0.0)
            saldo = max(disp_total - ja, 0.0)
            alocar = min(dem, saldo)
            df_n.at[i, 'Estoque'] = alocar
            usado[key] = ja + alocar

        df_n['DIF'] = (df_n['Nova demanda'] - df_n['Estoque']).clip(lower=0.0)

        if meses_N:
            lista_ped = []
            for _, r in df_pedidos.iterrows():
                key = r['chave']
                for m in meses_N:
                    qtd = float(r.get(m, 0.0) or 0.0)
                    if qtd > 0:
                        lista_ped.append((key, m, qtd))

            df_n['_o2'] = range(len(df_n))
            df_n = df_n.sort_values(['chave', 'MES_NEC', '_o2'])

            for key, mes, qtd_disp in lista_ped:
                elig = (df_n['chave'] == key) & (df_n['DIF'] > 0)
                for idx in df_n[elig].index:
                    if qtd_disp <= 0:
                        break
                    need = df_n.at[idx, 'DIF']
                    mes_nec = df_n.at[idx, 'MES_NEC']
                    destino = pd.Timestamp(max(mes, mes_nec))
                    if destino not in df_n.columns:
                        df_n[destino] = 0.0
                    df_n[destino] = df_n[destino].astype(float)
                    COLS_N.add(destino)
                    alocar = min(need, qtd_disp)
                    df_n.at[idx, destino] += alocar
                    df_n.at[idx, 'DIF'] -= alocar
                    qtd_disp -= alocar

            df_n = df_n.sort_values('_o2').drop(columns=['_o2'])

        df_n['REQUISIÇÕES'] = 0.0
        reqN = dict(zip(
            df_requisicoes['chave'],
            pd.to_numeric(df_requisicoes.get('TOTAL', df_requisicoes.iloc[:, 4]), errors='coerce').fillna(0.0)
        ))

        usado_req = {}
        meses_lista = sorted(COLS_N)

        for i, row in df_n.iterrows():
            key = row['chave']
            soma_ped = row[meses_lista].sum() if meses_lista else 0.0
            falta = row['Nova demanda'] - row['Estoque'] - soma_ped
            if falta <= 0:
                continue
            disp_total = reqN.get(key, 0.0)
            ja = usado_req.get(key, 0.0)
            saldo = max(disp_total - ja, 0.0)
            alocar = min(falta, saldo)
            df_n.at[i, 'REQUISIÇÕES'] = alocar
            usado_req[key] = ja + alocar

        soma_ped = df_n[meses_lista].sum(axis=1) if meses_lista else 0.0
        df_n['DESCOBERTO'] = (
            df_n['Nova demanda']
            - df_n['Estoque']
            - df_n['REQUISIÇÕES']
            - soma_ped
        ).clip(lower=0.0)

    df_final = (
        pd.concat([df_q, df_n], ignore_index=True)
        .sort_values(['EMPRESA', 'DATA NECESSIDADE'])
        .reset_index(drop=True)
    )

    for col in ['Estoque', 'Estoque Q', 'REQUISIÇÕES', 'REQUISIÇÕES Q']:
        if col not in df_final.columns:
            df_final[col] = 0.0

    df_final['ESTOQUE TOTAL'] = df_final['Estoque'].fillna(0) + df_final['Estoque Q'].fillna(0)
    df_final['REQUISIÇÕES TOTAL'] = df_final['REQUISIÇÕES'].fillna(0) + df_final['REQUISIÇÕES Q'].fillna(0)

    if 'DATA NECESSIDADE' in df_final.columns:
        df_final['DATA NECESSIDADE'] = df_final['DATA NECESSIDADE'].dt.strftime('%d/%m/%Y')

    if 'VERSÃO SEMANA' in df_final.columns:
        df_final['VERSÃO SEMANA'] = df_final['VERSÃO SEMANA'].dt.strftime('%d/%m/%Y')

    if 'VERSÃO PLANO' in df_final.columns:
        df_final['VERSÃO PLANO'] = df_final['VERSÃO PLANO'].dt.strftime('%d/%m/%Y')

    novos_nomes = {}
    usados = set()

    meses_pt = {
        "jan": "jan", "feb": "fev", "mar": "mar", "apr": "abr",
        "may": "mai", "jun": "jun", "jul": "jul", "aug": "ago",
        "sep": "set", "oct": "out", "nov": "nov", "dec": "dez"
    }

    for col in df_final.columns:
        dt = pd.to_datetime(col, errors='coerce', dayfirst=True)
        if not pd.isna(dt):
            mes_en = dt.strftime('%b').lower()
            ano = dt.strftime('%y')
            mes_pt = meses_pt.get(mes_en, mes_en)
            novo = f"{mes_pt}/{ano}"
            if novo in usados:
                i = 1
                while f"{novo}_{i}" in usados:
                    i += 1
                novo = f"{novo}_{i}"
            usados.add(novo)
            novos_nomes[col] = novo

    if novos_nomes:
        df_final = df_final.rename(columns=novos_nomes)

    for col in df_final.columns:
        try:
            datetime.strptime(col, "%b/%y")
            df_final[col] = df_final[col].fillna(0).round().astype(int)
        except:
            pass

    df_final.to_excel(ARQ_SAIDA, index=False)
    print(f"\n✅ Resultado salvo com sucesso em: {ARQ_SAIDA}")
    print(f"⏱️ Tempo total: {time.time() - start_time:.2f} segundos")

if __name__ == '__main__':
    executar()

 
