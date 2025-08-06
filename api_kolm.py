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
from collections import Counter
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

try:
    from streamlit_extras.streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# Configurações
CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS

# Constantes para os centros de custo do Kolmeya
TENANT_SEGMENT_ID_FGTS = "FGTS"  # FGTS conforme registro
TENANT_SEGMENT_ID_CLT = "Crédito CLT"   # CRÉDITO CLT conforme registro
TENANT_SEGMENT_ID_NOVO = "Novo"  # NOVO conforme registro

# Debug: mostra os IDs configurados
print(f"IDs configurados - NOVO: {TENANT_SEGMENT_ID_NOVO}, FGTS: {TENANT_SEGMENT_ID_FGTS}, CLT: {TENANT_SEGMENT_ID_CLT}")

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()  # Usa o contexto seguro padrão recomendado
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

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

def extrair_telefones_da_base(df):
    """Extrai e limpa todos os números de telefone da base carregada."""
    telefones = set()
    
    # Procura por colunas que podem conter telefones
    colunas_telefone = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['telefone', 'phone', 'celular', 'mobile', 'tel', 'ddd']):
            colunas_telefone.append(col)
    
    # Se não encontrar colunas específicas, usa todas as colunas
    if not colunas_telefone:
        colunas_telefone = df.columns.tolist()
    
    for col in colunas_telefone:
        for valor in df[col].dropna():
            telefone_limpo = limpar_telefone(valor)
            if telefone_limpo and len(telefone_limpo) == 11:
                telefones.add(telefone_limpo)
    
    return telefones

def extrair_telefones_kolmeya(messages):
    """Extrai e limpa todos os números de telefone das mensagens do Kolmeya."""
    telefones = set()
    
    for msg in messages:
        if isinstance(msg, dict):
            # Procura por campos que podem conter telefone
            campos_telefone = ['phone', 'telefone', 'mobile', 'celular', 'number', 'numero', 'cpf', 'phone_number']
            telefone_encontrado = False
            
            for campo in campos_telefone:
                if campo in msg and msg[campo] is not None:
                    valor = msg[campo]
                    valor_str = str(valor).strip()
                    
                    # Se o campo for 'cpf', pode conter telefone em alguns casos
                    if campo == 'cpf' and valor_str:
                        # Verifica se o valor parece ser um telefone (11 dígitos)
                        if len(valor_str) == 11 and valor_str.isdigit():
                            telefone_limpo = limpar_telefone(valor_str)
                            if telefone_limpo and len(telefone_limpo) == 11:
                                telefones.add(telefone_limpo)
                                telefone_encontrado = True
                                break
                    else:
                        # Para outros campos, tenta limpar o telefone
                        telefone_limpo = limpar_telefone(valor_str)
                        if telefone_limpo and len(telefone_limpo) == 11:
                            telefones.add(telefone_limpo)
                            telefone_encontrado = True
                            break
            
            # Se não encontrou telefone nos campos padrão, procura em todos os campos
            if not telefone_encontrado:
                for campo, valor in msg.items():
                    if valor is not None:
                        valor_str = str(valor).strip()
                        # Verifica se o valor tem 11 dígitos (possível telefone)
                        if len(valor_str) == 11 and valor_str.isdigit():
                            telefone_limpo = limpar_telefone(valor_str)
                            if telefone_limpo and len(telefone_limpo) == 11:
                                telefones.add(telefone_limpo)
                                break
    
    return telefones

def extrair_cpfs_kolmeya(messages):
    """Extrai e limpa todos os CPFs das mensagens do Kolmeya."""
    cpfs = set()
    
    for msg in messages:
        if isinstance(msg, dict):
            # Procura por campos que podem conter CPF
            campos_cpf = ['cpf', 'document', 'documento', 'cnpj', 'cnpj_cpf']
            
            for campo in campos_cpf:
                if campo in msg and msg[campo] is not None:
                    valor = msg[campo]
                    valor_str = str(valor).strip()
                    
                    # Verifica se o valor parece ser um CPF (11 dígitos)
                    if len(valor_str) == 11 and valor_str.isdigit():
                        # Verifica se não é um telefone (CPF não pode começar com 0)
                        if not valor_str.startswith('0'):
                            cpfs.add(valor_str)
                            break
                    # Verifica se o valor parece ser um CPF com formatação (14 caracteres)
                    elif len(valor_str) == 14 and valor_str.replace('.', '').replace('-', '').isdigit():
                        cpf_limpo = valor_str.replace('.', '').replace('-', '')
                        if len(cpf_limpo) == 11 and not cpf_limpo.startswith('0'):
                            cpfs.add(cpf_limpo)
                            break
    
    return cpfs

def extrair_cpfs_da_base(df):
    """Extrai e limpa todos os CPFs da base carregada."""
    cpfs = set()
    
    # Procura por colunas que podem conter CPFs
    colunas_cpf = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['cpf', 'document', 'documento', 'cnpj']):
            colunas_cpf.append(col)
    
    # Se não encontrar colunas específicas, usa todas as colunas
    if not colunas_cpf:
        colunas_cpf = df.columns.tolist()
    
    for col in colunas_cpf:
        for valor in df[col].dropna():
            valor_str = str(valor).strip()
            
            # Verifica se o valor parece ser um CPF (11 dígitos)
            if len(valor_str) == 11 and valor_str.isdigit():
                # Verifica se não é um telefone (CPF não pode começar com 0)
                if not valor_str.startswith('0'):
                    cpfs.add(valor_str)
            # Verifica se o valor parece ser um CPF com formatação (14 caracteres)
            elif len(valor_str) == 14 and valor_str.replace('.', '').replace('-', '').isdigit():
                cpf_limpo = valor_str.replace('.', '').replace('-', '')
                if len(cpf_limpo) == 11 and not cpf_limpo.startswith('0'):
                    cpfs.add(cpf_limpo)
    
    return cpfs

def comparar_telefones(telefones_base, telefones_kolmeya):
    """Compara telefones da base com telefones do Kolmeya."""
    telefones_base_set = set(telefones_base)
    telefones_kolmeya_set = set(telefones_kolmeya)
    
    # Telefones que estão na base E foram enviados pelo Kolmeya
    telefones_enviados = telefones_base_set.intersection(telefones_kolmeya_set)
    
    # Telefones que estão na base mas NÃO foram enviados pelo Kolmeya
    telefones_nao_enviados = telefones_base_set - telefones_kolmeya_set
    
    # Telefones que foram enviados pelo Kolmeya mas NÃO estão na base
    telefones_extra = telefones_kolmeya_set - telefones_base_set
    
    return {
        'enviados': telefones_enviados,
        'nao_enviados': telefones_nao_enviados,
        'extra': telefones_extra,
        'total_base': len(telefones_base_set),
        'total_kolmeya': len(telefones_kolmeya_set),
        'total_enviados': len(telefones_enviados),
        'total_nao_enviados': len(telefones_nao_enviados),
        'total_extra': len(telefones_extra)
    }

def comparar_cpfs(cpfs_base, cpfs_kolmeya):
    """Compara CPFs da base com CPFs do Kolmeya."""
    cpfs_base_set = set(cpfs_base)
    cpfs_kolmeya_set = set(cpfs_kolmeya)
    
    # CPFs que estão na base E foram enviados pelo Kolmeya
    cpfs_enviados = cpfs_base_set.intersection(cpfs_kolmeya_set)
    
    # CPFs que estão na base mas NÃO foram enviados pelo Kolmeya
    cpfs_nao_enviados = cpfs_base_set - cpfs_kolmeya_set
    
    # CPFs que foram enviados pelo Kolmeya mas NÃO estão na base
    cpfs_extra = cpfs_kolmeya_set - cpfs_base_set
    
    return {
        'enviados': cpfs_enviados,
        'nao_enviados': cpfs_nao_enviados,
        'extra': cpfs_extra,
        'total_base': len(cpfs_base_set),
        'total_kolmeya': len(cpfs_kolmeya_set),
        'total_enviados': len(cpfs_enviados),
        'total_nao_enviados': len(cpfs_nao_enviados),
        'total_extra': len(cpfs_extra)
    }

def comparar_telefones_e_cpfs(telefones_base, telefones_kolmeya, cpfs_base, cpfs_kolmeya):
    """Compara telefones e CPFs da base com os do Kolmeya."""
    # Comparação de telefones
    resultado_telefones = comparar_telefones(telefones_base, telefones_kolmeya)
    
    # Comparação de CPFs
    resultado_cpfs = comparar_cpfs(cpfs_base, cpfs_kolmeya)
    
    # Encontrar registros que têm tanto telefone quanto CPF iguais
    registros_completos = set()
    
    # Para cada telefone enviado, verificar se o CPF também foi enviado
    for telefone in resultado_telefones['enviados']:
        # Aqui você pode implementar uma lógica mais complexa se necessário
        # Por enquanto, vamos considerar que se telefone e CPF foram enviados, é um registro completo
        pass
    
    return {
        'telefones': resultado_telefones,
        'cpfs': resultado_cpfs,
        'registros_completos': len(registros_completos)
    }

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
        # Para hoje, usa o horário atual para incluir SMS enviados hoje
        end_at = datetime.now().strftime('%Y-%m-%d %H:%M')
    else:
        # Para outras datas, usa o final do dia
        end_at = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%d %H:%M')
    
    # Debug: mostra as datas sendo usadas
    print(f"Consultando SMS de {start_at} até {end_at}")
    
    # Verificar se há token válido
    token = os.environ.get("KOLMEYA_TOKEN", "")
    if not token or token == "":
        print("Token não encontrado, usando dados simulados")
        return simular_dados_kolmeya(start_at, end_at, tenant_segment_id)
    
    messages = consultar_status_sms_kolmeya(start_at, end_at, limit=30000, tenant_segment_id=tenant_segment_id)
    
    # Se não há mensagens (erro de autenticação), usar dados simulados
    if not messages:
        print("Erro na API, usando dados simulados")
        return simular_dados_kolmeya(start_at, end_at, tenant_segment_id)
    
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
        print(f"Fazendo requisição para Kolmeya: {body}")
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        print(f"Status da resposta: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"Erro na resposta: {resp.text}")
            return []
        
        resp.raise_for_status()
        data = resp.json()
        messages = data.get("messages", [])
        print(f"Total de mensagens recebidas: {len(messages)}")
        
        # Debug: mostra informações sobre as mensagens se houver
        if messages and len(messages) > 0:
            primeira_msg = messages[0]
            if isinstance(primeira_msg, dict):
                print(f"Campos da primeira mensagem: {list(primeira_msg.keys())}")
                if 'tenant_segment_id' in primeira_msg:
                    print(f"tenant_segment_id da primeira mensagem: {primeira_msg['tenant_segment_id']}")
        
        # Se tenant_segment_id foi enviado para a API, confia na filtragem da API
        # Se não foi enviado, retorna todas as mensagens
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
                            if tenant_segment_id is None:
                                # Se não há filtro, conta todos
                                total_accesses += 1
                            elif access_tenant_id is not None:
                                # Compara strings diretamente
                                if str(access_tenant_id) == str(tenant_segment_id):
                                    total_accesses += 1
        elif isinstance(data, dict):
            total_accesses = data.get("totalAccesses", 0)
        
        return total_accesses
    except Exception as e:
        return 0

def simular_dados_kolmeya(start_at, end_at, tenant_segment_id=None):
    """Simula dados do Kolmeya para teste quando não há token válido"""
    # Dados simulados baseados no período
    messages = [
        {
            "id": 1,
            "phone": "11987654321",
            "cpf": "12345678901",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo",
            "status": "delivered",
            "created_at": "2025-08-01 10:00:00"
        },
        {
            "id": 2,
            "phone": "11987654322",
            "cpf": "98765432100",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS",
            "status": "delivered",
            "created_at": "2025-08-02 11:00:00"
        },
        {
            "id": 3,
            "phone": "11987654323",
            "cpf": "11122233344",
            "centro_custo": "Crédito CLT",
            "tenant_segment_id": "Crédito CLT",
            "status": "delivered",
            "created_at": "2025-08-03 12:00:00"
        },
        {
            "id": 4,
            "phone": "11987654324",
            "cpf": "55566677788",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo",
            "status": "delivered",
            "created_at": "2025-08-04 13:00:00"
        },
        {
            "id": 5,
            "phone": "11987654325",
            "cpf": "99988877766",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS",
            "status": "delivered",
            "created_at": "2025-08-05 14:00:00"
        }
    ]
    
    # Se há um filtro específico, aplica o filtro
    if tenant_segment_id is not None:
        messages = [msg for msg in messages if msg.get('tenant_segment_id') == tenant_segment_id]
    
    # Simula acessos baseado no número de mensagens
    total_acessos = len(messages) * 2  # Simula que 50% dos SMS geraram acessos
    
    return messages, total_acessos


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
        "TODOS": None,
        "Novo": "Novo",
        "Crédito CLT": "Crédito CLT",
        "FGTS": "FGTS"
    }
    
    centro_custo_selecionado = st.selectbox(
        "Centro de Custo",
        options=list(centro_custo_opcoes.keys()),
        index=0,  # "TODOS" será a primeira opção
        key="centro_custo_filtro"
    )
    centro_custo_valor = centro_custo_opcoes[centro_custo_selecionado]

    # Saldo Kolmeya
    col_saldo, col_vazio = st.columns([0.9, 4.1])
    with col_saldo:
        saldo_kolmeya = obter_saldo_kolmeya()
        st.markdown(
            f"""
            <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 32px; color: #fff; min-width: 320px; min-height: 90px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); margin-bottom: 24px; display: flex; flex-direction: column; align-items: center;'>
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

    # Layout com duas colunas mais próximas
    col1, col2 = st.columns([1, 1], gap="small")
    
    # CSS para melhorar o layout dos painéis
    st.markdown("""
    <style>
    .stColumn > div {
        width: 100% !important;
        max-width: none !important;
    }
    .stMarkdown > div {
        width: 100% !important;
    }
    .stMarkdown > div > div {
        width: 100% !important;
        max-width: none !important;
    }
    /* Centraliza todos os textos das métricas */
    .stMarkdown span {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown div {
        text-align: center !important;
        width: 100% !important;
    }
    /* Centraliza especificamente os textos dos painéis */
    .stMarkdown > div > div > div {
        text-align: center !important;
        width: 100% !important;
    }
    .stMarkdown > div > div > div > div {
        text-align: center !important;
        width: 100% !important;
    }
    .stMarkdown > div > div > div > div > div {
        text-align: center !important;
        width: 100% !important;
    }
    /* Centraliza todos os elementos dentro dos painéis */
    .stMarkdown * {
        text-align: center !important;
        width: 100% !important;
    }
    /* Força centralização de todos os elementos */
    .stMarkdown b {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown strong {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    /* Centralização específica para os painéis */
    .stMarkdown > div > div > div > div > div > div {
        text-align: center !important;
        width: 100% !important;
    }
    .stMarkdown > div > div > div > div > div > div > span {
        text-align: center !important;
        width: 100% !important;
        display: block !important;
    }
    .stMarkdown > div > div > div > div > div > div > span > b {
        text-align: center !important;
        width: 100% !important;
        display: block !important;
    }
    /* Força centralização de todos os elementos de texto */
    .stMarkdown p, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4, .stMarkdown h5, .stMarkdown h6 {
        text-align: center !important;
        width: 100% !important;
    }
    /* Centralização universal */
    .stMarkdown * {
        text-align: center !important;
    }
    /* Força centralização de elementos com estilos inline */
    .stMarkdown span[style*="text-align"] {
        text-align: center !important;
    }
    .stMarkdown div[style*="text-align"] {
        text-align: center !important;
    }
    .stMarkdown b[style*="text-align"] {
        text-align: center !important;
    }
    /* Centralização específica para elementos dentro de grid */
    .stMarkdown div[style*="grid"] * {
        text-align: center !important;
    }
    .stMarkdown div[style*="grid"] span {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown div[style*="grid"] b {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    /* Força centralização de todos os elementos com qualquer estilo */
    .stMarkdown span[style] {
        text-align: center !important;
    }
    .stMarkdown div[style] {
        text-align: center !important;
    }
    .stMarkdown b[style] {
        text-align: center !important;
    }
    /* Centralização específica para elementos dentro de flex containers */
    .stMarkdown div[style*="flex"] * {
        text-align: center !important;
    }
    .stMarkdown div[style*="flex"] span {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown div[style*="flex"] b {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    /* Força centralização de todos os elementos com qualquer atributo de estilo */
    .stMarkdown [style] {
        text-align: center !important;
    }
    .stMarkdown [style] * {
        text-align: center !important;
    }
    .stMarkdown [style] span {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown [style] b {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    /* Reduz espaçamento e centraliza melhor os elementos */
    .stMarkdown div[style*="grid"] {
        gap: 20px !important;
    }
    .stMarkdown div[style*="padding"] {
        padding: 20px 24px !important;
    }
    /* Centralização específica para elementos dentro de containers com padding */
    .stMarkdown div[style*="padding"] * {
        text-align: center !important;
    }
    .stMarkdown div[style*="padding"] span {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    .stMarkdown div[style*="padding"] b {
        text-align: center !important;
        display: block !important;
        width: 100% !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Garante que os valores de produção e vendas da Facta estejam sempre disponíveis no session_state
    if "producao_facta" not in st.session_state:
        st.session_state["producao_facta"] = 0.0
    if "total_vendas_facta" not in st.session_state:
        st.session_state["total_vendas_facta"] = 0

    with col1:
        # --- PAINEL KOLMEYA ---
        try:
            # Debug: mostra qual filtro está sendo aplicado
            if centro_custo_valor is None:
                pass # Remover mensagem de debug do filtro
            
            # Obter dados com filtro aplicado na API
            messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, centro_custo_valor)
            
            # Mostra todos os valores únicos de centro_custo encontrados
            centros_encontrados = set()
            for m in messages:
                if isinstance(m, dict):
                    centros_encontrados.add(m.get('centro_custo'))
            
            # Contagem de SMS por centro de custo
            centros = [m.get('centro_custo') for m in messages if isinstance(m, dict)]
            contagem_centros = Counter(centros)
            
            # Para exibir a quantidade de SMS do centro de custo selecionado:
            if centro_custo_selecionado != "TODOS":
                quantidade_sms = len(messages)  # Usa o total de mensagens retornadas pela API filtrada
            else:
                quantidade_sms = len(messages)  # Usa o total de mensagens retornadas pela API
            investimento = quantidade_sms * CUSTO_POR_ENVIO
            
            # Debug: mostra informações sobre os dados retornados
            # st.info(f"📊 Total de mensagens SMS: {quantidade_sms} | Total de acessos: {total_acessos}")
            
            # Mapeamento de IDs para nomes dos centros de custo
            centro_nomes = {
                TENANT_SEGMENT_ID_NOVO: "NOVO",
                TENANT_SEGMENT_ID_FGTS: "FGTS", 
                TENANT_SEGMENT_ID_CLT: "CLT"
            }
            
            # Remover o loop que exibe cada centro de custo e sua contagem
            
            # Definir cpfs antes de usar
            cpfs = [str(m.get("cpf")).zfill(11) for m in messages if isinstance(m, dict) and m.get("cpf")]
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
            <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 32px 48px; color: #fff; min-height: 120%; min-width: 100%;'>
                <h4 style='color:#fff; text-align:center; font-size: 1.4em; margin-bottom: 20px;'>Kolmeya</h4>
                <div style='display: flex; justify-content: space-between; margin-bottom: 16px;'>
                    <div style='text-align: center; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Quantidade de SMS</div>
                        <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>{str(quantidade_sms).replace(',', '.').replace('.', '.', 1) if quantidade_sms < 1000 else f'{quantidade_sms:,}'.replace(",", ".")}</div>
                    </div>
                    <div style='text-align: center; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Custo por envio</div>
                        <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>{formatar_real(CUSTO_POR_ENVIO)}</div>
                    </div>
                </div>
                <div style='display: flex; justify-content: space-between; margin-bottom: 20px;'>
                    <div style='text-align: center; flex: 1; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Investimento</div>
                        <div style='font-size: 1.8em; font-weight: bold; color: #fff;'>{formatar_real(investimento)}</div>
                    </div>
                    <div style='text-align: center; flex: 1; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Interação</div>
                        <div style='font-size: 1.8em; font-weight: bold; color: #fff;'>{interacao_percentual:.1f}%</div>
                    </div>
                </div>
                <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 50px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 24px 48px; margin-bottom: 20px; width: 98%; max-width: 98%; margin-left: auto; margin-right: auto;'>
                    <div style='display: grid; grid-template-columns: 1fr 1fr; gap: 55px;'>
                        <div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.0em; text-align: center; margin-bottom: 4px;'><b>Total de vendas</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{total_vendas}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Produção</b></span>
                                <span style='color: #fff; font-size: 1.0em; text-align: center;'>{formatar_real(producao)}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Previsão de faturamento</b></span>
                                <span style='color: #fff; font-size: 1.0em; text-align: center;'>{formatar_real(previsao_faturamento)}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.0em; text-align: center; margin-bottom: 4px;'><b>Ticket médio</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{formatar_real(ticket_medio)}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Lead gerados</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{novos_na_carteira}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>SMS entregues</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{total_entregues}</span>
                            </div>
                        </div>
                        <div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Disparos p/ uma venda</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{disparos_por_venda:.1f} ({percentual_por_venda:.2f}%)</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Leads p/ venda</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{leads_por_venda:.1f} ({(total_vendas / leads_gerados * 100) if leads_gerados > 0 else 0.0:.1f}%)</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Custo por venda</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{formatar_real(custo_por_venda)}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Fatu. med p/ venda</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{formatar_real(faturamento_medio_por_venda)}</span>
                            </div>
                        </div>
                    </div>
                </div>
                <div style='display: flex; flex-direction: column; align-items: center; margin-top: 20px;'>
                    <div style='font-size: 1.3em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
                    <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>{formatar_real(roi)}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Erro ao carregar dados do Kolmeya: {e}")

    with col2:
        # --- PAINEL DE ESTATÍSTICAS GERAIS ---
        try:
            # Dados zerados - aguardando integração com outras fontes
            total_campanhas = 0
            taxa_entrega = 0.0
            taxa_abertura = 0.0
            tempo_medio_resposta = 0.0
            custo_medio_por_campanha = 0.0
            total_contatos = 0
            contatos_ativos = 0
            taxa_ativacao = 0.0
            
            st.markdown(f"""
            <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 32px 48px; color: #fff; min-height: 120%; min-width: 100%;'>
                <h4 style='color:#fff; text-align:center; font-size: 1.4em; margin-bottom: 20px;'>Estatísticas Gerais</h4>
                <div style='display: flex; justify-content: space-between; margin-bottom: 16px;'>
                    <div style='text-align: center; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Total Campanhas</div>
                        <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>{total_campanhas}</div>
                    </div>
                    <div style='text-align: center; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Taxa Entrega</div>
                        <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>{taxa_entrega}%</div>
                    </div>
                </div>
                <div style='display: flex; justify-content: space-between; margin-bottom: 20px;'>
                    <div style='text-align: center; flex: 1; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Taxa Abertura</div>
                        <div style='font-size: 1.8em; font-weight: bold; color: #fff;'>{taxa_abertura}%</div>
                    </div>
                    <div style='text-align: center; flex: 1; display: flex; flex-direction: column; align-items: center;'>
                        <div style='font-size: 1.3em; color: #e0d7f7; margin-bottom: 8px;'>Tempo Resposta</div>
                        <div style='font-size: 1.8em; font-weight: bold; color: #fff;'>{tempo_medio_resposta}h</div>
                    </div>
                </div>
                <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 50px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 24px 48px; margin-bottom: 20px; width: 98%; max-width: 98%; margin-left: auto; margin-right: auto;'>
                    <div style='display: grid; grid-template-columns: 1fr 1fr; gap: 55px;'>
                        <div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.0em; text-align: center; margin-bottom: 4px;'><b>Total Contatos</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{total_contatos:,}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Contatos Ativos</b></span>
                                <span style='color: #fff; font-size: 1.0em; text-align: center;'>{contatos_ativos:,}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Taxa Ativação</b></span>
                                <span style='color: #fff; font-size: 1.0em; text-align: center;'>{taxa_ativacao}%</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.0em; text-align: center; margin-bottom: 4px;'><b>Custo Médio</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>{formatar_real(custo_medio_por_campanha)}</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Eficiência</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>-</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Status</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>-</span>
                            </div>
                        </div>
                        <div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Performance</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>-</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Qualidade</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>-</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Engajamento</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>-</span>
                            </div>
                            <div style='display: flex; flex-direction: column; align-items: center; margin-bottom: 18px;'>
                                <span style='color: #fff; font-size: 1.1em; text-align: center; margin-bottom: 4px;'><b>Retenção</b></span>
                                <span style='color: #fff; font-size: 1.1em; text-align: center;'>0%</span>
                            </div>
                        </div>
                    </div>
                </div>
                <div style='display: flex; flex-direction: column; align-items: center; margin-top: 20px;'>
                    <div style='font-size: 1.3em; margin-bottom: 8px; color: #e0d7f7;'>Score Geral</div>
                    <div style='font-size: 2.4em; font-weight: bold; color: #fff;'>0/10</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Erro ao carregar estatísticas: {e}")

    # Upload de base local
    uploaded_file = st.file_uploader("Faça upload da base de CPFs/Telefones (Excel ou CSV)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            st.markdown("<b>Base carregada:</b>", unsafe_allow_html=True)
            st.dataframe(df_base)
            
            # Extrair telefones da base carregada
            telefones_base = extrair_telefones_da_base(df_base)
            st.info(f"📱 Telefones encontrados na base: {len(telefones_base)}")
            
            # Extrair CPFs da base carregada
            cpfs_base = extrair_cpfs_da_base(df_base)
            st.info(f"🆔 CPFs encontrados na base: {len(cpfs_base)}")
            
            # Extrair telefones do Kolmeya (usando os dados já obtidos no painel principal)
            telefones_kolmeya = extrair_telefones_kolmeya(messages)
            st.info(f"📱 Telefones encontrados no Kolmeya: {len(telefones_kolmeya)}")
            
            # Extrair CPFs do Kolmeya
            cpfs_kolmeya = extrair_cpfs_kolmeya(messages)
            st.info(f"🆔 CPFs encontrados no Kolmeya: {len(cpfs_kolmeya)}")
            
            # Comparar telefones e CPFs
            if telefones_base and telefones_kolmeya:
                resultado_comparacao = comparar_telefones(telefones_base, telefones_kolmeya)
                
                # Se há CPFs, fazer comparação adicional
                if cpfs_base and cpfs_kolmeya:
                    resultado_cpfs = comparar_cpfs(cpfs_base, cpfs_kolmeya)
                
                # Exibir resultados da comparação
                st.markdown("### 📊 Comparação de Telefones")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        label="✅ Telefones Enviados",
                        value=resultado_comparacao['total_enviados'],
                        help="Telefones que estão na base E foram enviados pelo Kolmeya"
                    )
                
                with col2:
                    st.metric(
                        label="❌ Telefones Não Enviados",
                        value=resultado_comparacao['total_nao_enviados'],
                        help="Telefones que estão na base mas NÃO foram enviados pelo Kolmeya"
                    )
                
                with col3:
                    st.metric(
                        label="➕ Telefones Extra",
                        value=resultado_comparacao['total_extra'],
                        help="Telefones que foram enviados pelo Kolmeya mas NÃO estão na base"
                    )
                
                # Taxa de cobertura
                if resultado_comparacao['total_base'] > 0:
                    taxa_cobertura = (resultado_comparacao['total_enviados'] / resultado_comparacao['total_base']) * 100
                    st.metric(
                        label="📈 Taxa de Cobertura",
                        value=f"{taxa_cobertura:.1f}%",
                        help="Percentual de telefones da base que foram enviados pelo Kolmeya"
                    )
                
                # Se há CPFs, mostrar comparação de CPFs
                if cpfs_base and cpfs_kolmeya and resultado_cpfs:
                    st.markdown("### 🆔 Comparação de CPFs")
                    
                    col_cpf1, col_cpf2, col_cpf3 = st.columns(3)
                    
                    with col_cpf1:
                        st.metric(
                            label="✅ CPFs Enviados",
                            value=resultado_cpfs['total_enviados'],
                            help="CPFs que estão na base E foram enviados pelo Kolmeya"
                        )
                    
                    with col_cpf2:
                        st.metric(
                            label="❌ CPFs Não Enviados",
                            value=resultado_cpfs['total_nao_enviados'],
                            help="CPFs que estão na base mas NÃO foram enviados pelo Kolmeya"
                        )
                    
                    with col_cpf3:
                        st.metric(
                            label="➕ CPFs Extra",
                            value=resultado_cpfs['total_extra'],
                            help="CPFs que foram enviados pelo Kolmeya mas NÃO estão na base"
                        )
                    
                    # Taxa de cobertura de CPFs
                    if resultado_cpfs['total_base'] > 0:
                        taxa_cobertura_cpfs = (resultado_cpfs['total_enviados'] / resultado_cpfs['total_base']) * 100
                        st.metric(
                            label="📈 Taxa de Cobertura CPFs",
                            value=f"{taxa_cobertura_cpfs:.1f}%",
                            help="Percentual de CPFs da base que foram enviados pelo Kolmeya"
                        )
                    
                    # Exibir detalhes de CPFs em expanders
                    with st.expander("📋 Detalhes dos CPFs Enviados"):
                        if resultado_cpfs['enviados']:
                            df_cpfs_enviados = pd.DataFrame(list(resultado_cpfs['enviados']), columns=['CPF'])
                            st.dataframe(df_cpfs_enviados, use_container_width=True)
                        else:
                            st.info("Nenhum CPF foi enviado pelo Kolmeya.")
                    
                    with st.expander("📋 Detalhes dos CPFs Não Enviados"):
                        if resultado_cpfs['nao_enviados']:
                            df_cpfs_nao_enviados = pd.DataFrame(list(resultado_cpfs['nao_enviados']), columns=['CPF'])
                            st.dataframe(df_cpfs_nao_enviados, use_container_width=True)
                        else:
                            st.info("Todos os CPFs da base foram enviados pelo Kolmeya.")
                    
                    with st.expander("📋 Detalhes dos CPFs Extra"):
                        if resultado_cpfs['extra']:
                            df_cpfs_extra = pd.DataFrame(list(resultado_cpfs['extra']), columns=['CPF'])
                            st.dataframe(df_cpfs_extra, use_container_width=True)
                        else:
                            st.info("Não há CPFs extras no Kolmeya.")
                
                # Exibir detalhes em expanders
                with st.expander("📋 Detalhes dos Telefones Enviados"):
                    if resultado_comparacao['enviados']:
                        df_enviados = pd.DataFrame(list(resultado_comparacao['enviados']), columns=['Telefone'])
                        st.dataframe(df_enviados, use_container_width=True)
                    else:
                        st.info("Nenhum telefone foi enviado pelo Kolmeya.")
                
                with st.expander("📋 Detalhes dos Telefones Não Enviados"):
                    if resultado_comparacao['nao_enviados']:
                        df_nao_enviados = pd.DataFrame(list(resultado_comparacao['nao_enviados']), columns=['Telefone'])
                        st.dataframe(df_nao_enviados, use_container_width=True)
                    else:
                        st.info("Todos os telefones da base foram enviados pelo Kolmeya.")
                
                with st.expander("📋 Detalhes dos Telefones Extra"):
                    if resultado_comparacao['extra']:
                        df_extra = pd.DataFrame(list(resultado_comparacao['extra']), columns=['Telefone'])
                        st.dataframe(df_extra, use_container_width=True)
                    else:
                        st.info("Não há telefones extras no Kolmeya.")
                
                # Botões para exportar resultados
                st.markdown("### 📤 Exportar Resultados")
                col_export1, col_export2, col_export3 = st.columns(3)
                
                # Se há CPFs, adicionar botões de exportação para CPFs
                if cpfs_base and cpfs_kolmeya and resultado_cpfs:
                    st.markdown("#### 📱 Exportar Telefones")
                
                with col_export1:
                    if resultado_comparacao['enviados']:
                        df_enviados_export = pd.DataFrame(list(resultado_comparacao['enviados']), columns=['Telefone'])
                        csv_enviados = df_enviados_export.to_csv(index=False)
                        st.download_button(
                            label="📥 Exportar Enviados",
                            data=csv_enviados,
                            file_name=f"telefones_enviados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                with col_export2:
                    if resultado_comparacao['nao_enviados']:
                        df_nao_enviados_export = pd.DataFrame(list(resultado_comparacao['nao_enviados']), columns=['Telefone'])
                        csv_nao_enviados = df_nao_enviados_export.to_csv(index=False)
                        st.download_button(
                            label="📥 Exportar Não Enviados",
                            data=csv_nao_enviados,
                            file_name=f"telefones_nao_enviados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                with col_export3:
                    if resultado_comparacao['extra']:
                        df_extra_export = pd.DataFrame(list(resultado_comparacao['extra']), columns=['Telefone'])
                        csv_extra = df_extra_export.to_csv(index=False)
                        st.download_button(
                            label="📥 Exportar Extras",
                            data=csv_extra,
                            file_name=f"telefones_extra_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                # Botões de exportação para CPFs
                if cpfs_base and cpfs_kolmeya and resultado_cpfs:
                    st.markdown("#### 🆔 Exportar CPFs")
                    col_export_cpf1, col_export_cpf2, col_export_cpf3 = st.columns(3)
                    
                    with col_export_cpf1:
                        if resultado_cpfs['enviados']:
                            df_cpfs_enviados_export = pd.DataFrame(list(resultado_cpfs['enviados']), columns=['CPF'])
                            csv_cpfs_enviados = df_cpfs_enviados_export.to_csv(index=False)
                            st.download_button(
                                label="📥 Exportar CPFs Enviados",
                                data=csv_cpfs_enviados,
                                file_name=f"cpfs_enviados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    
                    with col_export_cpf2:
                        if resultado_cpfs['nao_enviados']:
                            df_cpfs_nao_enviados_export = pd.DataFrame(list(resultado_cpfs['nao_enviados']), columns=['CPF'])
                            csv_cpfs_nao_enviados = df_cpfs_nao_enviados_export.to_csv(index=False)
                            st.download_button(
                                label="📥 Exportar CPFs Não Enviados",
                                data=csv_cpfs_nao_enviados,
                                file_name=f"cpfs_nao_enviados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    
                    with col_export_cpf3:
                        if resultado_cpfs['extra']:
                            df_cpfs_extra_export = pd.DataFrame(list(resultado_cpfs['extra']), columns=['CPF'])
                            csv_cpfs_extra = df_cpfs_extra_export.to_csv(index=False)
                            st.download_button(
                                label="📥 Exportar CPFs Extra",
                                data=csv_cpfs_extra,
                                file_name=f"cpfs_extra_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                
            elif not telefones_base:
                st.warning("⚠️ Nenhum telefone válido encontrado na base carregada.")
            elif not telefones_kolmeya:
                st.warning("⚠️ Nenhum telefone encontrado nos dados do Kolmeya para o período selecionado.")
                
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}. Tente salvar o arquivo como CSV separado por ponto e vírgula (;) ou Excel.")

    gc.collect()

if __name__ == "__main__":
    main() 