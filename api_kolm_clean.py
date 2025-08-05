import os
import requests
import streamlit as st
from datetime import datetime, timedelta, date
import re
import pandas as pd
import io
import gc
import json
import time
import urllib3
import ssl
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from streamlit_extras.streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# Configurações
CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS
CUSTO_POR_LIGACAO_URA = 0.034444  # R$ 0,034444 por ligação URA

# Constantes para os centros de custo do Kolmeya
TENANT_SEGMENT_ID_FGTS = 8103  # FGTS conforme registro
TENANT_SEGMENT_ID_CLT = 8208   # CRÉDITO CLT conforme registro
TENANT_SEGMENT_ID_NOVO = 8105  # NOVO conforme registro

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

def limpar_telefone(telefone):
    if not telefone:
        return ""
    t = re.sub(r'\D', '', str(telefone))
    # Mantém apenas os 11 últimos dígitos (ignora DDI, zeros à esquerda, etc)
    if len(t) >= 11:
        return t[-11:]
    return ""

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def obter_saldo_kolmeya(token=None):
    """Consulta o saldo disponível do Kolmeya via endpoint /api/v1/sms/balance."""
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

def obter_dados_sms_com_filtro(data_ini, data_fim, tenant_segment_id=None):
    """Consulta o endpoint Kolmeya para status de SMS."""
    if data_ini is None or data_fim is None:
        return [], 0
    
    start_at = datetime.combine(data_ini, datetime.min.time()).strftime('%Y-%m-%d %H:%M')
    
    hoje = datetime.now().date()
    if data_fim == hoje:
        end_at = datetime.now().strftime('%Y-%m-%d %H:%M')
    else:
        end_at = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%d %H:%M')
    
    messages = consultar_status_sms_kolmeya(start_at, end_at, limit=30000, tenant_segment_id=tenant_segment_id)
    
    start_at_acessos = data_ini.strftime('%Y-%m-%d')
    end_at_acessos = data_fim.strftime('%Y-%m-%d')
    total_acessos = consultar_acessos_sms_kolmeya(start_at_acessos, end_at_acessos, limit=5000, tenant_segment_id=tenant_segment_id)
    
    return messages, total_acessos

def consultar_status_sms_kolmeya(start_at, end_at, limit=30000, token=None, tenant_segment_id=None):
    """Consulta o status das mensagens SMS enviadas via Kolmeya."""
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
    if tenant_segment_id is not None:
        body["tenant_segment_id"] = tenant_segment_id
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        if resp.status_code != 200:
            return []
        resp.raise_for_status()
        data = resp.json()
        messages = data.get("messages", [])
        
        if tenant_segment_id is not None and messages:
            messages_filtradas = []
            for msg in messages:
                if isinstance(msg, dict):
                    msg_tenant_id = msg.get('tenant_segment_id')
                    if msg_tenant_id is not None:
                        try:
                            msg_tenant_id_int = int(msg_tenant_id)
                            tenant_segment_id_int = int(tenant_segment_id)
                            if msg_tenant_id_int == tenant_segment_id_int:
                                messages_filtradas.append(msg)
                        except (ValueError, TypeError):
                            pass
            return messages_filtradas
        return messages
    except Exception as e:
        print(f"Erro ao consultar status SMS Kolmeya: {e}")
        return []

def consultar_acessos_sms_kolmeya(start_at, end_at, limit=5000, token=None, tenant_segment_id=None):
    """Consulta os acessos das mensagens SMS via Kolmeya."""
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
        
        total_accesses = 0
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "accesses" in item:
                    accesses_list = item.get("accesses", [])
                    for access in accesses_list:
                        if isinstance(access, dict):
                            access_tenant_id = access.get('tenant_segment_id')
                            if tenant_segment_id is None or access_tenant_id == tenant_segment_id:
                                total_accesses += 1
        elif isinstance(data, dict):
            total_accesses = data.get("totalAccesses", 0)
        
        return total_accesses
    except Exception as e:
        return 0

def obter_dados_ura(idCampanha, periodoInicial, periodoFinal, idTabulacao=None, idGrupoUsuario=None, idUsuario=None, idLote=None, exibirUltTabulacao=True):
    """Consulta o endpoint de ligações detalhadas da URA (Argus)."""
    argus_token = os.environ.get('ARGUS_TOKEN', '')
    
    if not argus_token:
        print("⚠️ AVISO: ARGUS_TOKEN não está configurado!")
        return {
            "codStatus": 0, 
            "descStatus": "Token não configurado", 
            "qtdeRegistros": 0, 
            "ligacoes": [],
            "quantidade_ura": 0,
            "custo_por_ligacao": 0.034444,
            "investimento": 0.0,
            "atendidas": 0,
            "total_vendas": 0,
            "producao": 0.0,
            "previsao_faturamento": 0.0,
            "ticket_medio": 0.0,
            "roi": 0.0,
            "percentual_atendem": 0.0,
            "leads_gerados": 0,
            "percentual_conversao_lead": 0.0,
            "ligacoes_por_lead": 0.0,
            "percentual_leads_converte_vendas": 0.0,
            "ligacoes_por_venda": 0.0,
            "custo_por_lead": 0.0,
            "custo_por_venda": 0.0,
            "faturamento_medio_por_venda": 0.0
        }
    
    url = "https://argus.app.br/apiargus"
    headers = {
        "Authorization": f"Bearer {argus_token}",
        "Content-Type": "application/json"
    }
    body = {
        "idCampanha": idCampanha,
        "periodoInicial": periodoInicial,
        "periodoFinal": periodoFinal
    }
    
    if idTabulacao is not None:
        body["idTabulacao"] = idTabulacao
    if idGrupoUsuario is not None:
        body["idGrupoUsuario"] = idGrupoUsuario
    if idUsuario is not None:
        body["idUsuario"] = idUsuario
    if idLote is not None:
        body["idLote"] = idLote
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30, verify=False)
        resp.raise_for_status()
        data = resp.json()
        
        tabulacoes = data.get("tabulacoes", [])
        if not tabulacoes:
            tabulacoes = data.get("ligacoes", [])
        if not tabulacoes:
            tabulacoes = data.get("dados", [])
        if not tabulacoes:
            if isinstance(data, list):
                tabulacoes = data
            else:
                tabulacoes = []
        
        return {
            "codStatus": data.get("codStatus", 0),
            "descStatus": data.get("descStatus", ""),
            "qtdeRegistros": len(tabulacoes),
            "ligacoes": tabulacoes,
            "quantidade_ura": len(tabulacoes),
            "custo_por_ligacao": 0.034444,
            "investimento": len(tabulacoes) * 0.034444,
            "atendidas": 0,
            "total_vendas": 0,
            "producao": 0.0,
            "previsao_faturamento": 0.0,
            "ticket_medio": 0.0,
            "roi": 0.0,
            "percentual_atendem": 0.0,
            "leads_gerados": 0,
            "percentual_conversao_lead": 0.0,
            "ligacoes_por_lead": 0.0,
            "percentual_leads_converte_vendas": 0.0,
            "ligacoes_por_venda": 0.0,
            "custo_por_lead": 0.0,
            "custo_por_venda": 0.0,
            "faturamento_medio_por_venda": 0.0
        }
        
    except Exception as e:
        print(f"Erro URA: {e}")
        return {
            "codStatus": 0, 
            "descStatus": str(e), 
            "qtdeRegistros": 0, 
            "ligacoes": [],
            "quantidade_ura": 0,
            "custo_por_ligacao": 0.034444,
            "investimento": 0.0,
            "atendidas": 0,
            "total_vendas": 0,
            "producao": 0.0,
            "previsao_faturamento": 0.0,
            "ticket_medio": 0.0,
            "roi": 0.0,
            "percentual_atendem": 0.0,
            "leads_gerados": 0,
            "percentual_conversao_lead": 0.0,
            "ligacoes_por_lead": 0.0,
            "percentual_leads_converte_vendas": 0.0,
            "ligacoes_por_venda": 0.0,
            "custo_por_lead": 0.0,
            "custo_por_venda": 0.0,
            "faturamento_medio_por_venda": 0.0
        }

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
                    return pd.read_csv(uploaded_file, dtype=str)
                except Exception as e:
                    uploaded_file.seek(0)
                    raise e
    else:
        return pd.read_excel(uploaded_file, dtype=str)

def main():
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")
    
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # Campos de período
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

    # Saldo Kolmeya
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
        try:
            messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, tenant_segment_id_filtro)
            
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
            cpfs_unicos = set(cpfs)
            novos_na_carteira = total_acessos
            leads_gerados = novos_na_carteira
            
            # Produção e vendas vindos da Facta
            producao = st.session_state["producao_facta"]
            total_vendas = st.session_state["total_vendas_facta"]
            
            # Cálculos
            previsao_faturamento = producao * 0.171
            ticket_medio = producao / total_vendas if total_vendas > 0 else 0.0
            faturamento_medio_por_venda = previsao_faturamento / total_vendas if total_vendas > 0 else 0.0
            custo_por_venda = investimento / leads_gerados if leads_gerados > 0 else 0.0
            disparos_por_venda = quantidade_sms / total_vendas if total_vendas > 0 else 0.0
            percentual_por_venda = (total_vendas / quantidade_sms * 100) if quantidade_sms > 0 else 0.0
            disparos_por_lead = quantidade_sms / leads_gerados if leads_gerados > 0 else 0.0
            leads_por_venda = leads_gerados / total_vendas if total_vendas > 0 else 0.0
            roi = previsao_faturamento - investimento
            total_entregues = st.session_state.get('total_entregues', 0)
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
                        </div>
                        <div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Disparos p/ uma venda</b></span>
                                <span style='color: #fff;'>{disparos_por_venda:.1f}</span>
                            </div>
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
        except Exception as e:
            st.error(f"Erro ao carregar dados do Kolmeya: {e}")

    with col2:
        # --- PAINEL URA ---
        try:
            idCampanha = 1
            periodoInicial = (datetime.combine(data_ini, datetime.min.time()) - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00')
            periodoFinal = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%dT23:59:59')
            
            dados_ura = obter_dados_ura(idCampanha, periodoInicial, periodoFinal)
            
            ligacoes_all = dados_ura.get("ligacoes", [])
            quantidade_ura = len(ligacoes_all)
            custo_por_ligacao_ura = dados_ura.get("custo_por_ligacao", 0.034444)
            investimento_ura = quantidade_ura * custo_por_ligacao_ura
            
            # Processa ligações para identificar vendas e atendimentos
            total_vendas_ura = 0
            producao_ura = 0.0
            atendidas_ura = 0
            
            for ligacao in ligacoes_all:
                if isinstance(ligacao, dict):
                    tabulacao = str(ligacao.get('tabulado', '')).lower()
                    categoria_tabulacao = str(ligacao.get('categoriaTabulacao', '')).lower()
                    historico = str(ligacao.get('historico', '')).lower()
                    resultado = str(ligacao.get('resultadoLigacao', '')).lower()
                    
                    # Identifica vendas
                    if any(keyword in tabulacao for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                        total_vendas_ura += 1
                    if any(keyword in categoria_tabulacao for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                        total_vendas_ura += 1
                    if any(keyword in historico for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                        total_vendas_ura += 1
                    if any(keyword in resultado for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido', 'completada']):
                        total_vendas_ura += 1
                    
                    # Identifica atendimentos
                    if tabulacao != 'não tabulado' and tabulacao != 'nao tabulado' and tabulacao:
                        atendidas_ura += 1
                    if categoria_tabulacao != 'não tabulado' and categoria_tabulacao != 'nao tabulado' and categoria_tabulacao:
                        atendidas_ura += 1
                    if 'completada' in resultado:
                        atendidas_ura += 1
            
            # Remove duplicatas
            total_vendas_ura = min(total_vendas_ura, quantidade_ura)
            if atendidas_ura == 0:
                atendidas_ura = len(ligacoes_all)
            
            # Calcula métricas
            previsao_faturamento_ura = producao_ura * 0.171
            ticket_medio_ura = producao_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
            roi_ura = previsao_faturamento_ura - investimento_ura
            percentual_atendem_ura = (atendidas_ura / quantidade_ura * 100) if quantidade_ura > 0 else 0.0
            leads_gerados_ura = atendidas_ura
            percentual_conversao_lead_ura = (total_vendas_ura / leads_gerados_ura * 100) if leads_gerados_ura > 0 else 0.0
            ligacoes_por_lead_ura = quantidade_ura / leads_gerados_ura if leads_gerados_ura > 0 else 0.0
            percentual_leads_converte_vendas_ura = (total_vendas_ura / leads_gerados_ura * 100) if leads_gerados_ura > 0 else 0.0
            ligacoes_por_venda_ura = quantidade_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
            custo_por_lead_ura = investimento_ura / leads_gerados_ura if leads_gerados_ura > 0 else 0.0
            custo_por_venda_ura = investimento_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
            faturamento_medio_por_venda_ura = previsao_faturamento_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
            
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
                        <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(custo_por_ligacao_ura)}</div>
                    </div>
                </div>
                <div style='display: flex; justify-content: space-between; margin-bottom: 16px;'>
                    <div style='text-align: center; flex: 1;'>
                        <div style='font-size: 1.1em; color: #e0d7f7;'>Investimento</div>
                        <div style='font-size: 1.5em; font-weight: bold; color: #fff;'>{formatar_real(investimento_ura)}</div>
                    </div>
                    <div style='text-align: center; flex: 1;'>
                        <div style='font-size: 1.1em; color: #e0d7f7;'>Atendidas</div>
                        <div style='font-size: 1.5em; font-weight: bold; color: #fff;'>{atendidas_ura}</div>
                    </div>
                </div>
                <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                    <div style='display: grid; grid-template-columns: 1fr 1fr; gap: 16px;'>
                        <div>
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
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Ticket médio</b></span>
                                <span style='color: #fff;'>{formatar_real(ticket_medio_ura)}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Leads gerados</b></span>
                                <span style='color: #fff;'>{leads_gerados_ura}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Lig. atendidas p/ um lead</b></span>
                                <span style='color: #fff;'>{ligacoes_por_lead_ura:.1f}</span>
                            </div>
                        </div>
                        <div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Ligações p/uma venda</b></span>
                                <span style='color: #fff;'>{ligacoes_por_venda_ura:.1f}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>% p/ venda</b></span>
                                <span style='color: #fff;'>{percentual_leads_converte_vendas_ura:.1f}%</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Leads p/ venda</b></span>
                                <span style='color: #fff;'>{leads_gerados_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0:.1f}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Custo por lead</b></span>
                                <span style='color: #fff;'>{formatar_real(custo_por_lead_ura)}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Custo por venda</b></span>
                                <span style='color: #fff;'>{formatar_real(custo_por_venda_ura)}</span>
                            </div>
                            <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                                <span style='color: #fff;'><b>Fatu. med p/ venda</b></span>
                                <span style='color: #fff;'>{formatar_real(faturamento_medio_por_venda_ura)}</span>
                            </div>
                        </div>
                    </div>
                </div>
                <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
                <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(roi_ura)}</div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Erro ao carregar dados da URA: {e}")

    # Upload de base local
    uploaded_file = st.file_uploader("Faça upload da base de CPFs/Telefones (Excel ou CSV)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            st.markdown("<b>Base carregada:</b>", unsafe_allow_html=True)
            st.dataframe(df_base)
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}. Tente salvar o arquivo como CSV separado por ponto e vírgula (;) ou Excel.")

    gc.collect()

if __name__ == "__main__":
    main() 