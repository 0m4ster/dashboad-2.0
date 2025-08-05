import os
import requests
import streamlit as st
from datetime import datetime, timedelta, date
import re
import pandas as pd  # Adiciona pandas para compatibilidade com exemplo
import io
import gc
import json
try:
    from streamlit_extras.streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
import httpx  # Adicionado para garantir uso do httpx
import ssl
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# --- INÍCIO REMOÇÃO DE ENDPOINT E FUNÇÕES RELACIONADAS ---
# Removido: Funções obter_dados_sms, obter_dados_sms_periodo, obter_dados_sms_com_filtro e todas as requisições ao endpoint Kolmeya SMS Statuses
# --- FIM REMOÇÃO DE ENDPOINT E FUNÇÕES RELACIONADAS ---

CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS
CUSTO_POR_LIGACAO_URA = 0.034444  # R$ 0,034444 por ligação URA

# Constantes para os centros de custo do Kolmeya
TENANT_SEGMENT_ID_FGTS = 8103  # FGTS conforme registro
TENANT_SEGMENT_ID_CLT = 8208   # CRÉDITO CLT conforme registro
TENANT_SEGMENT_ID_NOVO = 8105  # NOVO conforme registro
CENTROS_CUSTO_KOLMEYA = [TENANT_SEGMENT_ID_FGTS, TENANT_SEGMENT_ID_CLT, TENANT_SEGMENT_ID_NOVO]

# Debug: Lista de IDs para testar para o centro NOVO
IDS_NOVO_PARA_TESTAR = [8105, 8104, 8106, 8107, 8108, 8109, 8110, 8111, 8112]

# Função para buscar SMS FGTS

def get_week_range(now):
    start_of_week = now - timedelta(days=now.weekday())  # Segunda-feira
    start_at = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6)
    end_at = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999)
    if end_at > now:
        end_at = now
    return start_at, end_at

def get_today_range(now):
    start_at = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=0, microsecond=0)
    end_at = min(end_of_day, now)
    return start_at, end_at

# Substitui função auxiliar por função dummy

def obter_dados_sms_com_filtro(data_ini, data_fim, tenant_segment_id=None):
    """
    Consulta o endpoint Kolmeya para status de SMS, usando o filtro de datas informado.
    Args:
        data_ini (date): Data inicial (date ou datetime)
        data_fim (date): Data final (date ou datetime)
        tenant_segment_id (int): ID do centro de custo para filtrar
    Returns:
        list: Lista de mensagens retornadas pela API
    """
    from datetime import datetime
    
    # Verifica se as datas são válidas
    if data_ini is None or data_fim is None:
        return []
    
    # Garante formato 'YYYY-MM-DD HH:MM'
    start_at = datetime.combine(data_ini, datetime.min.time()).strftime('%Y-%m-%d %H:%M')
    
    # Verifica se a data final é hoje
    hoje = datetime.now().date()
    if data_fim == hoje:
        # Se for hoje, usa a data/hora atual
        end_at = datetime.now().strftime('%Y-%m-%d %H:%M')
    else:
        # Se não for hoje, usa o final do dia
        end_at = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%d %H:%M')
    
    # Consulta SMS
    messages = consultar_status_sms_kolmeya(start_at, end_at, limit=30000, tenant_segment_id=tenant_segment_id)
    
    # Consulta acessos (formato de data diferente: YYYY-MM-DD)
    start_at_acessos = data_ini.strftime('%Y-%m-%d')
    end_at_acessos = data_fim.strftime('%Y-%m-%d')
    total_acessos = consultar_acessos_sms_kolmeya(start_at_acessos, end_at_acessos, limit=5000, tenant_segment_id=tenant_segment_id)
    
    return messages, total_acessos

def limpar_telefone(telefone):
    if not telefone:
        return ""
    t = re.sub(r'\D', '', str(telefone))
    # Mantém apenas os 11 últimos dígitos (ignora DDI, zeros à esquerda, etc)
    if len(t) >= 11:
        return t[-11:]
    return ""

# Função utilitária para formatar valores em Real

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def obter_propostas_facta(
    data_ini=None, data_fim=None, cpf=None, pagina=1, quantidade=5000, phpsessid=None,
    convenio=None, averbador=None, af=None, consulta_sub=None, codigo_sub=None,
    data_alteracao_ini=None, data_alteracao_fim=None
):
    """
    Consulta o endpoint andamento-propostas da Facta, paginando até trazer todos os resultados do período.
    Permite busca por vários parâmetros, inclusive grandes volumes.
    """
    # Tenta ler o token do arquivo salvo pelo gera_token_facta.py
    try:
        with open("facta_token.txt") as f:
            facta_token = f.read().strip()
    except Exception:
        facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    facta_env = os.environ.get('FACTA_ENV', 'prod').lower()
    if facta_env == 'homolog':
        url_base = "https://webservice-homol.facta.com.br/proposta/andamento-propostas"
    else:
        url_base = "https://webservice.facta.com.br/proposta/andamento-propostas"
    headers = {
        "Authorization": f"Bearer {facta_token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None

    # Datas padrão: últimos 30 dias
    if not data_fim:
        data_fim = datetime.now().strftime('%d/%m/%Y')
    if not data_ini:
        data_ini = (datetime.now() - timedelta(days=30)).strftime('%d/%m/%Y')

    propostas = []
    while True:
        params = {
            "data_ini": data_ini,
            "data_fim": data_fim,
            "pagina": pagina,
            "quantidade": quantidade
        }
        if cpf:
            params["cpf"] = cpf
        if convenio:
            params["convenio"] = convenio
        if averbador:
            params["averbador"] = averbador
        if af:
            params["af"] = af
        if consulta_sub:
            params["consulta_sub"] = consulta_sub
        if codigo_sub:
            params["codigo_sub"] = codigo_sub
        if data_alteracao_ini:
            params["data_alteracao_ini"] = data_alteracao_ini
        if data_alteracao_fim:
            params["data_alteracao_fim"] = data_alteracao_fim

        try:
            resp = requests.get(url_base, headers=headers, cookies=cookies, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if data.get("erro"):
                break
            propostas_page = data.get("propostas", [])
            propostas.extend(propostas_page)
            if len(propostas_page) < quantidade:
                break
            pagina += 1
        except Exception as e:
            print(f"Erro na consulta Facta: {e}")
            break
    return propostas

def gerar_blocos_datas(data_ini, data_fim, dias_bloco=30):
    blocos = []
    atual = data_ini
    while atual <= data_fim:
        proximo = min(atual + timedelta(days=dias_bloco-1), data_fim)
        blocos.append((atual, proximo))
        atual = proximo + timedelta(days=1)
    return blocos

def obter_dados_ura(idCampanha, periodoInicial, periodoFinal, idTabulacao=None, idGrupoUsuario=None, idUsuario=None, idLote=None, exibirUltTabulacao=True):
    """
    Consulta o endpoint de tabulações detalhadas da URA (Argus).
    """
    argus_token = os.environ.get('ARGUS_TOKEN', '')
    url = "https://argus.app.br/apiargus/report/tabulacoesdetalhadas"
    headers = {
        "Authorization": f"Bearer {argus_token}",
        "Content-Type": "application/json"
    }
    body = {
        "idCampanha": idCampanha,
        "periodoInicial": periodoInicial,
        "periodoFinal": periodoFinal,
        "exibirUltTabulacao": exibirUltTabulacao
    }
    if idTabulacao is not None:
        body["idTabulacao"] = idTabulacao
    if idGrupoUsuario is not None:
        body["idGrupoUsuario"] = idGrupoUsuario
    if idUsuario is not None:
        body["idUsuario"] = idUsuario
    if idLote is not None:
        body["idLote"] = idLote
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data
    except Exception as e:
        return {"codStatus": 0, "descStatus": str(e), "qtdeRegistros": 0, "tabulacoes": []}

def obter_resumo_jobs(periodo=None):
    pass  # Removido: função de requisição de geração de leads/acessos

@st.cache_data(ttl=600)
def ler_base(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        try:
            return pd.read_csv(uploaded_file, dtype=str, sep=';')
        except Exception:
            uploaded_file.seek(0)
            try:
                return pd.read_csv(uploaded_file, dtype=str, sep=',')
            except Exception:
                uploaded_file.seek(0)
                try:
                    return pd.read_csv(uploaded_file, dtype=str)  # Tenta o padrão do pandas
                except Exception as e:
                    uploaded_file.seek(0)
                    raise e
    else:
        return pd.read_excel(uploaded_file, dtype=str)

# Função para consultar o resumo dos jobs do Kolmeya

def obter_resumo_jobs_kolmeya(period, token=None):
    pass  # Removido: função de requisição de geração de leads/acessos

def obter_saldo_kolmeya(token=None):
    """
    Consulta o saldo disponível do Kolmeya via endpoint /api/v1/sms/balance.
    """
    if token is None:
        token = os.environ.get("KOLMEYA_TOKEN", "")
    url = "https://kolmeya.com.br/api/v1/sms/balance"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    try:
        resp = requests.post(url, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("balance")
    except Exception as e:
        return f"Erro ao consultar saldo: {e}"

# Removido: função obter_total_acessos_kolmeya e todas as suas chamadas

def buscar_valor_af_cpfs(cpfs, data_ini=None, data_fim=None, max_workers=2):
    """
    Consulta a Facta dividindo em lotes e processando em paralelo controlado.
    Divide a lista em 4 partes, cada uma processada em paralelo, depois junta os resultados.
    """
    if len(cpfs) <= 10:  # Se poucos CPFs, processa sequencialmente
        return buscar_valor_af_cpfs_sequencial(cpfs, data_ini, data_fim)
    
    # Divide a lista em 4 lotes
    tamanho_lote = len(cpfs) // 4
    lote1 = cpfs[:tamanho_lote]
    lote2 = cpfs[tamanho_lote:tamanho_lote*2]
    lote3 = cpfs[tamanho_lote*2:tamanho_lote*3]
    lote4 = cpfs[tamanho_lote*3:]  # Último lote pega o resto
    
    resultados = {}
    
    def processar_lote(cpfs_lote):
        resultados_lote = {}
        for cpf in cpfs_lote:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    propostas = obter_propostas_facta(cpf=cpf, data_ini=data_ini, data_fim=data_fim)
                    # Filtra apenas propostas com status "16 - CONTRATO PAGO"
                    propostas_filtradas = [p for p in propostas if p.get('status_proposta') == '16 - CONTRATO PAGO']
                    valores_af = [p.get('valor_af') for p in propostas_filtradas if 'valor_af' in p]
                    time.sleep(0.1)  # Delay para evitar sobrecarga na API
                    resultados_lote[cpf] = valores_af
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        print(f"Erro ao consultar CPF {cpf} após {max_retries} tentativas: {e}")
                        resultados_lote[cpf] = []
                    time.sleep(0.5)  # Delay maior entre retries
        return resultados_lote
    
    # Processa os 4 lotes em paralelo
    with ThreadPoolExecutor(max_workers=4) as executor:
        future1 = executor.submit(processar_lote, lote1)
        future2 = executor.submit(processar_lote, lote2)
        future3 = executor.submit(processar_lote, lote3)
        future4 = executor.submit(processar_lote, lote4)
        
        # Junta os resultados
        resultados.update(future1.result())
        resultados.update(future2.result())
        resultados.update(future3.result())
        resultados.update(future4.result())
    
    return resultados

def buscar_valor_af_cpfs_sequencial(cpfs, data_ini=None, data_fim=None):
    """
    Consulta a Facta sequencialmente para cada CPF (usado para poucos CPFs).
    """
    resultados = {}
    for cpf in cpfs:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                propostas = obter_propostas_facta(cpf=cpf, data_ini=data_ini, data_fim=data_fim)
                # Filtra apenas propostas com status "16 - CONTRATO PAGO"
                propostas_filtradas = [p for p in propostas if p.get('status_proposta') == '16 - CONTRATO PAGO']
                valores_af = [p.get('valor_af') for p in propostas_filtradas if 'valor_af' in p]
                resultados[cpf] = valores_af
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Erro ao consultar CPF {cpf} após {max_retries} tentativas: {e}")
                    resultados[cpf] = []
                time.sleep(0.5)
    return resultados

def calcular_total_vendas(valores_af):
    """
    Calcula e retorna a quantidade total de vendas baseada nos valores_af.
    
    Args:
        valores_af (dict): Dicionário com CPFs como chaves e listas de valores como valores
    
    Returns:
        int: Quantidade total de vendas
    """
    total_vendas = 0
    
    for cpf, valores in valores_af.items():
        if valores:  # Se há valores para este CPF
            total_vendas += len(valores)  # Conta cada valor como uma venda
    
    return total_vendas

def main():
    # Campos de período para todo o dashboard
    col_data_ini, col_data_fim = st.columns(2)
    with col_data_ini:
        data_ini = st.date_input("Data inicial", value=datetime.now().replace(day=1).date(), key="data_ini_topo")
    with col_data_fim:
        data_fim = st.date_input("Data final", value=datetime.now().date(), key="data_fim_topo")

    # Filtro de centro de custo
    centro_custo_opcoes = {
        "NOVO": None,
        "FGTS": TENANT_SEGMENT_ID_FGTS,
        "CLT": TENANT_SEGMENT_ID_CLT
    }
    
    centro_custo_selecionado = st.selectbox(
        "Centro de Custo",
        options=list(centro_custo_opcoes.keys()),
        index=0,
        key="centro_custo_filtro"
    )
    tenant_segment_id_filtro = centro_custo_opcoes[centro_custo_selecionado]

    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")  # Atualiza a cada 2 minutos
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # NOVO: Linha de colunas para o saldo Kolmeya (completamente à esquerda)
    col_saldo, col_vazio = st.columns([0.9, 4.1])
    with col_saldo:
        saldo_kolmeya = obter_saldo_kolmeya()
        st.markdown(
            f"""
            <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 32px; color: #fff; min-width: 320px; min-height: 90px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); margin-bottom: 24px;'>
                <div style='font-size: 1.3em; color: #e0d7f7; font-weight: bold; margin-bottom: 8px;'>Saldo Atual Kolmeya</div>
                <div style='font-size: 2.5em; font-weight: bold; color: #fff;'>
                    {formatar_real(float(saldo_kolmeya)) if saldo_kolmeya and str(saldo_kolmeya).replace(",", ".").replace(".", "", 1).replace("-", "").isdigit() else saldo_kolmeya}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown(
        """
        <style>
        body {
            background-color: #181624 !important;
        }
        .stApp {
            background-color: #181624 !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    col1, col2 = st.columns([3, 2])
    # Garante que os valores de produção e vendas da Facta estejam sempre disponíveis no session_state
    if "producao_facta" not in st.session_state:
        st.session_state["producao_facta"] = 0.0
    if "total_vendas_facta" not in st.session_state:
        st.session_state["total_vendas_facta"] = 0
    
    with col1:
        # --- PAINEL KOLMEYA ---
        # Substitui chamada por lista vazia e aviso
        messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, tenant_segment_id_filtro)
        
        # Aviso removido
        if not messages:
            quantidade_sms = 0
            investimento = 0.0
            telefones = []
            cpfs = []
        else:
            quantidade_sms = len(messages)
            investimento = quantidade_sms * CUSTO_POR_ENVIO
            telefones = []
            for m in messages:
                tel = m.get('telefone') if isinstance(m, dict) else None
                if tel:
                    telefones.append(limpar_telefone(tel))
            cpfs = [str(m.get("cpf")).zfill(11) for m in messages if isinstance(m, dict) and m.get("cpf")]
        
        # --- NOVOS NA CARTEIRA ---
        # Calcula baseado na quantidade de CPFs únicos encontrados nos SMS
        cpfs_unicos = set(cpfs)
        
        # Usa total de acessos como "Novos na carteira"
        novos_na_carteira = total_acessos
        
        # Usa "Novos na carteira" como leads gerados
        leads_gerados = novos_na_carteira
        
        # Produção e vendas vindos da Facta, se já consultados
        producao = st.session_state["producao_facta"]
        total_vendas = st.session_state["total_vendas_facta"]
        # Previsão de faturamento = produção * 0,171
        previsao_faturamento = producao * 0.171
        # Ticket médio = produção / total de vendas
        ticket_medio = producao / total_vendas if total_vendas > 0 else 0.0
        # Faturamento médio por venda = previsão de faturamento / total de vendas
        faturamento_medio_por_venda = previsao_faturamento / total_vendas if total_vendas > 0 else 0.0
        # Custo por venda = investimento / leads gerados
        custo_por_venda = investimento / leads_gerados if leads_gerados > 0 else 0.0
        # Disparos p/ uma venda = quantidade de SMS / total de vendas
        disparos_por_venda = quantidade_sms / total_vendas if total_vendas > 0 else 0.0
        # % p/ venda = total de venda / quantidade de sms
        percentual_por_venda = (total_vendas / quantidade_sms * 100) if quantidade_sms > 0 else 0.0
        # Disparos p/ um lead = quantidade de sms / por leads gerados
        disparos_por_lead = quantidade_sms / leads_gerados if leads_gerados > 0 else 0.0
        # Leads p/ venda = leads gerados / total vendas
        leads_por_venda = leads_gerados / total_vendas if total_vendas > 0 else 0.0
        roi = previsao_faturamento - investimento
        total_entregues = st.session_state.get('total_entregues', 0)
        # Calcula a porcentagem de interação (leads gerados em relação ao total de SMS)
        interacao_percentual = (leads_gerados / quantidade_sms * 100) if quantidade_sms > 0 else 0
        st.markdown(f"""
        <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 16px; color: #fff; min-height: 100%;'>
            <h4 style='color:#fff; text-align:center;'>Kolmeya</h4>
            <div style='display: flex; justify-content: space-between; margin-bottom: 12px;'>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Quantidade de SMS</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{str(quantidade_sms).replace(',', '.').replace('.', '.', 1) if quantidade_sms < 1000 else f'{quantidade_sms:,}'.replace(",", ".")}</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Custo por envio</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(CUSTO_POR_ENVIO)}</div>
                </div>
            </div>
            <div style='display: flex; justify-content: space-between; margin-bottom: 16px;'>
                <div style='text-align: center; flex: 1;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Investimento</div>
                    <div style='font-size: 1.5em; font-weight: bold; color: #fff;'>{formatar_real(investimento)}</div>
                </div>
                <div style='text-align: center; flex: 1;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Interação</div>
                    <div style='font-size: 1.5em; font-weight: bold; color: #fff;'>{interacao_percentual:.1f}%</div>
                </div>
            </div>
            <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                <div style='display: grid; grid-template-columns: 1fr 1fr; gap: 16px;'>
                    <div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Total de vendas</b></span>
                            <span style='color: #fff;'>{total_vendas}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Produção</b></span>
                            <span style='color: #fff;'>{formatar_real(producao)}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Previsão de faturamento</b></span>
                            <span style='color: #fff;'>{formatar_real(previsao_faturamento)}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Ticket médio</b></span>
                            <span style='color: #fff;'>{formatar_real(ticket_medio)}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Lead gerados</b></span>
                            <span style='color: #fff;'>{novos_na_carteira}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>SMS entregues</b></span>
                            <span style='color: #fff;'>{total_entregues}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Disparos p/ uma venda</b></span>
                            <span style='color: #fff;'>{disparos_por_venda:.1f}</span>
                        </div>
                    </div>
                    <div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>% p/ venda</b></span>
                            <span style='color: #fff;'>{percentual_por_venda:.1f}%</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Leads p/ venda</b></span>
                            <span style='color: #fff;'>{leads_por_venda:.1f}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Custo por venda</b></span>
                            <span style='color: #fff;'>{formatar_real(custo_por_venda)}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Fatu. med p/ venda</b></span>
                            <span style='color: #fff;'>{formatar_real(faturamento_medio_por_venda)}</span>
                        </div>
                    </div>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
            <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(roi)}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        # --- PAINEL URA ---
        CUSTO_POR_LIGACAO_URA = 0.034444
        idCampanha = 1  # Fixo
        periodoInicial = (datetime.combine(data_ini, datetime.min.time()) - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00')
        periodoFinal = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%dT23:59:59')
        dados_ura = obter_dados_ura(idCampanha, periodoInicial, periodoFinal)
        quantidade_ura = dados_ura.get("qtdeRegistros", 0)
        investimento_ura = quantidade_ura * CUSTO_POR_LIGACAO_URA
        # Os campos abaixo são placeholders, ajuste conforme sua lógica de vendas/produção URA
        producao_ura = 0.0
        total_vendas_ura = 0
        previsao_faturamento_ura = 0.0
        ticket_medio_ura = 0.0
        roi_ura = previsao_faturamento_ura - investimento_ura
        st.markdown(f"""
        <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 16px; color: #fff; min-height: 100%;'>
            <h4 style='color:#fff; text-align:center;'>URA</h4>
            <div style='display: flex; justify-content: space-between; margin-bottom: 12px;'>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Quantidade de URA</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{str(quantidade_ura).replace(',', '.').replace('.', '.', 1) if quantidade_ura < 1000 else f'{quantidade_ura:,}'.replace(",", ".")}</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Custo por ligação</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(CUSTO_POR_LIGACAO_URA)}</div>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>Investimento</div>
            <div style='font-size: 2em; font-weight: bold; margin-bottom: 16px; color: #fff;'>{formatar_real(investimento_ura)}</div>
            <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Total de vendas</b></span>
                    <span style='color: #fff;'>{total_vendas_ura}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Produção</b></span>
                    <span style='color: #fff;'>{formatar_real(producao_ura)}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Previsão de faturamento</b></span>
                    <span style='color: #fff;'>{formatar_real(previsao_faturamento_ura)}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <span style='color: #fff;'><b>Ticket médio</b></span>
                    <span style='color: #fff;'>{formatar_real(ticket_medio_ura)}</span>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
            <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(roi_ura)}</div>
        </div>
        """, unsafe_allow_html=True)

    # --- BUSCAR TELEFONES DOS JOBS EM AMBOS ---
    telefones_em_ambos = set()  # Garante que sempre existe
    jobs_em_ambos = set()
    if messages and jobs_em_ambos:
        df_status = pd.DataFrame(messages)
        if 'job' in df_status.columns and 'telefone' in df_status.columns:
            mask = df_status['job'].apply(lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else None).isin(jobs_em_ambos)
            telefones_em_ambos = set(df_status.loc[mask, 'telefone'].dropna().astype(str))
    # --- UPLOAD DE BASE LOCAL ---
    uploaded_file = st.file_uploader("Faça upload da base de CPFs/Telefones (Excel ou CSV)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            st.markdown("<b>Base carregada:</b>", unsafe_allow_html=True)
            st.dataframe(df_base)
            # --- NOVO: Consulta valor_af na Facta apenas para CPFs cujos telefones aparecem no Kolmeya ---
            if 'Telefone' in df_base.columns and 'CPF' in df_base.columns:
                telefones_base = df_base['Telefone'].dropna().astype(str).map(limpar_telefone)
                telefones_kolmeya = set()
                for m in messages:
                    tel = m.get('telefone') if isinstance(m, dict) else None
                    if tel:
                        telefones_kolmeya.add(limpar_telefone(tel))
                cpfs_filtrados = df_base[telefones_base.isin(telefones_kolmeya)]['CPF'].dropna().astype(str).str.zfill(11)
                cpfs_filtrados = list(set(cpfs_filtrados))  # Remove duplicatas
                st.info("Consultando valor_af na Facta apenas para os CPFs cujos telefones aparecem no Kolmeya. Aguarde...")
                valores_af = buscar_valor_af_cpfs(cpfs_filtrados)
                import pandas as pd
                df_valores_af = pd.DataFrame([
                    {"CPF": cpf, "valor_af": ", ".join(map(str, valores)) if valores else "Não encontrado"}
                    for cpf, valores in valores_af.items()
                ])
                st.write("Valores 'valor_af' encontrados na Facta para cada CPF (apenas telefones presentes no Kolmeya):")
                st.dataframe(df_valores_af)
                # --- NOVO: Soma dos valores encontrados para produção ---
                import re
                total_producao = 0.0
                for valores in valores_af.values():
                    for v in valores:
                        if v is None:
                            continue
                        v_str = str(v).replace(' ', '').replace(',', '.')
                        # Extrai apenas números e ponto
                        match = re.match(r'^-?\d+(\.\d+)?$', v_str)
                        if match:
                            try:
                                total_producao += float(v_str)
                            except Exception:
                                pass
                st.session_state["producao_facta"] = total_producao
                
                # --- NOVO: Calcula o total de vendas ---
                total_vendas = calcular_total_vendas(valores_af)
                st.session_state["total_vendas_facta"] = total_vendas
                
                # Exibe o valor total encontrado para produção
                if total_producao > 0:
                    st.success(f"Total de produção (soma dos valor_af encontrados): R$ {total_producao:,.2f}")
                else:
                    st.warning("Nenhum valor válido de produção encontrado (valor_af). Verifique os dados retornados.")
                
                # Exibe o total de vendas
                if total_vendas > 0:
                    st.success(f"Total de vendas: {total_vendas}")
                else:
                    st.warning("Nenhuma venda encontrada. Verifique os dados retornados.")
                
                # Força atualização do painel Kolmeya
                st.rerun()
            # --- COMPARAÇÃO DE TELEFONES DA BASE COM O RELATÓRIO DE STATUS ---
            # Buscar todos os telefones do relatório de status (Kolmeya)
            telefones_kolmeya = set()
            for m in messages:
                tel = m.get('telefone') if isinstance(m, dict) else None
                if tel:
                    telefones_kolmeya.add(limpar_telefone(tel))
            # Padronizar telefones da base
            telefones_base = set()
            if 'Telefone' in df_base.columns:
                telefones_base = set(df_base['Telefone'].dropna().astype(str).map(limpar_telefone))
            # Comparar
            telefones_iguais = telefones_base & telefones_kolmeya
            st.markdown(f"<b>Total de telefones da base que aparecem no Kolmeya:</b> <span style='color:#e0d7f7;font-weight:bold;'>{len(telefones_iguais)}</span>", unsafe_allow_html=True)
            if telefones_iguais:
                st.write("Telefones encontrados:")
                st.write(sorted(telefones_iguais))
                # Exibir CPFs correspondentes na base
                if 'Telefone' in df_base.columns and 'CPF' in df_base.columns:
                    df_cpfs_base = df_base[df_base['Telefone'].astype(str).map(limpar_telefone).isin(telefones_iguais)][['Telefone', 'CPF']]
                    st.write("CPFs correspondentes na base:")
                    st.dataframe(df_cpfs_base)
                # Exibir CPFs correspondentes no Kolmeya
                cpfs_kolmeya = []
                for m in messages:
                    tel = m.get('telefone') if isinstance(m, dict) else None
                    cpf = m.get('cpf') if isinstance(m, dict) else None
                    if tel and limpar_telefone(tel) in telefones_iguais:
                        cpfs_kolmeya.append({'Telefone': limpar_telefone(tel), 'CPF': str(cpf).zfill(11) if cpf else None})
                if cpfs_kolmeya:
                    st.write("CPFs correspondentes no Kolmeya:")
                    st.dataframe(pd.DataFrame(cpfs_kolmeya))
            # --- FIM COMPARAÇÃO ---
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}. Tente salvar o arquivo como CSV separado por ponto e vírgula (;) ou Excel.")

    gc.collect()

def consultar_acessos_sms_kolmeya(start_at, end_at, limit=5000, token=None, tenant_segment_id=None):
    """
    Consulta os acessos das mensagens SMS via Kolmeya.
    Args:
        start_at (str): Data inicial no formato 'YYYY-MM-DD'.
        end_at (str): Data final no formato 'YYYY-MM-DD'.
        limit (int): Limite de registros (máx 5000).
        token (str): Token de autenticação. Se None, lê do env KOLMEYA_TOKEN.
        tenant_segment_id (int): ID do centro de custo para filtrar.
    Returns:
        int: Total de acessos retornado pela API.
    """
    import os
    import requests
    
    if token is None:
        token = os.environ.get("KOLMEYA_TOKEN", "")
    
    if not token:
        return 0
    
    url = "https://kolmeya.com.br/api/v1/sms/accesses"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    body = {
        "start_at": start_at,
        "end_at": end_at,
        "is_robot": 0,
        "limit": limit
    }
    if tenant_segment_id is not None:
        body["tenant_segment_id"] = tenant_segment_id
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        
        if resp.status_code != 200:
            return 0
            
        resp.raise_for_status()
        data = resp.json()
        
        # A API retorna uma lista de objetos, cada um com totalAccesses
        # Precisamos filtrar por tenant_segment_id se fornecido
        total_accesses = 0
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "accesses" in item:
                    accesses_list = item.get("accesses", [])
                    
                    # Filtra por tenant_segment_id se fornecido
                    for access in accesses_list:
                        if isinstance(access, dict):
                            access_tenant_id = access.get('tenant_segment_id')
                            if tenant_segment_id is None or access_tenant_id == tenant_segment_id:
                                total_accesses += 1
        elif isinstance(data, dict):
            # Fallback para caso a API mude e retorne um objeto único
            total_accesses = data.get("totalAccesses", 0)
        
        return total_accesses
    except Exception as e:
        return 0

def consultar_status_sms_kolmeya(start_at, end_at, limit=30000, token=None, tenant_segment_id=None):
    """
    Consulta o status das mensagens SMS enviadas via Kolmeya.
    Args:
        start_at (str): Data inicial no formato 'YYYY-MM-DD HH:MM'.
        end_at (str): Data final no formato 'YYYY-MM-DD HH:MM'.
        limit (int): Limite de registros (máx 30000).
        token (str): Token de autenticação. Se None, lê do env KOLMEYA_TOKEN.
        tenant_segment_id (int): ID do centro de custo para filtrar.
    Returns:
        list: Lista de mensagens retornadas pela API.
    """
    import os
    import requests
    if token is None:
        token = os.environ.get("KOLMEYA_TOKEN", "")
    url = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    body = {
        "start_at": start_at,
        "end_at": end_at,
        "limit": limit
    }
    # Adiciona tenant_segment_id se fornecido
    if tenant_segment_id is not None:
        body["tenant_segment_id"] = tenant_segment_id
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        
        if resp.status_code != 200:
            return []
            
        resp.raise_for_status()
        data = resp.json()
        messages = data.get("messages", [])
        
        # Filtragem local se tenant_segment_id foi fornecido
        if tenant_segment_id is not None and messages:
            messages_filtradas = []
            
            for i, msg in enumerate(messages):
                if isinstance(msg, dict):
                    msg_tenant_id = msg.get('tenant_segment_id')
                    
                    # Converte para int se necessário para comparação
                    if msg_tenant_id is not None:
                        try:
                            msg_tenant_id_int = int(msg_tenant_id)
                            tenant_segment_id_int = int(tenant_segment_id)
                            if msg_tenant_id_int == tenant_segment_id_int:
                                messages_filtradas.append(msg)
                        except (ValueError, TypeError) as e:
                            pass
                    else:
                        pass
            
            return messages_filtradas
            
        return messages
    except Exception as e:
        print(f"Erro ao consultar status SMS Kolmeya: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Response Text: {e.response.text}")
        return []

if __name__ == "__main__":
    main()