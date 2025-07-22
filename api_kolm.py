import os
import requests
import streamlit as st
from datetime import datetime, timedelta
import re
import pandas as pd  # Adiciona pandas para compatibilidade com exemplo
try:
    from streamlit_extras.streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
import httpx  # Adicionado para garantir uso do httpx
import ssl
print("OpenSSL version:", ssl.OPENSSL_VERSION)

API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS
CUSTO_POR_LIGACAO_URA = 0.034444  # R$ 0,034444 por ligação URA

# Função para buscar SMS FGTS

def get_week_range():
    hoje = datetime.now()
    start_of_week = hoje - timedelta(days=hoje.weekday())  # Segunda-feira
    start_at = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6)
    end_at = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999)
    now = datetime.now()
    if end_at > now:
        end_at = now
    return start_at, end_at

def get_today_range():
    hoje = datetime.now()
    start_at = hoje.replace(hour=0, minute=0, second=0, microsecond=0)
    now = datetime.now()
    end_of_day = hoje.replace(hour=23, minute=59, second=0, microsecond=0)
    end_at = min(end_of_day, now)
    return start_at, end_at

@st.cache_data(ttl=120)
def obter_dados_sms():
    from datetime import datetime, timedelta
    token = os.environ.get("KOLMEYA_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    agora = datetime.now()
    # Data máxima permitida pela API Kolmeya
    max_end_at = datetime(2025, 7, 22, 16, 2)
    if agora > max_end_at:
        agora = max_end_at
    sete_dias_atras = agora - timedelta(days=7)
    start_at = sete_dias_atras.replace(second=0, microsecond=0)
    end_at = agora.replace(second=0, microsecond=0)
    all_messages = []
    body = {
        "start_at": start_at.strftime('%Y-%m-%d %H:%M'),
        "end_at": end_at.strftime('%Y-%m-%d %H:%M'),
        "limit": 30000
    }
    try:
        resp = requests.post(API_URL, headers=headers, json=body, timeout=20)
        if resp.status_code == 422:
            try:
                st.error(f"Erro 422 na API Kolmeya: {resp.text}")
            except Exception:
                st.error("Erro 422 na API Kolmeya (não foi possível exibir o texto da resposta)")
            return []
        resp.raise_for_status()
        messages = resp.json().get("messages", [])
        all_messages.extend(messages)
    except Exception as e:
        st.error(f"Erro ao buscar dados da API: {e}")
        return []
    return all_messages

def limpar_telefone(telefone):
    if not telefone:
        return ""
    return re.sub(r"\D", "", str(telefone)) 

# Função utilitária para formatar valores em Real

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def obter_propostas_facta_ultimos_7_dias(phpsessid=None):
    """
    Busca propostas FGTS no endpoint andamento-propostas da Facta dos últimos 7 dias, com paginação automática.
    """
    from datetime import datetime, timedelta
    facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    url = "https://webservice.facta.com.br/proposta/andamento-propostas"
    headers = {
        "Authorization": f"Bearer {facta_token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None
    propostas_filtradas = []
    pagina = 1
    hoje = datetime.now()
    data_ini = (hoje - timedelta(days=7)).strftime('%d/%m/%Y')
    data_fim = hoje.strftime('%d/%m/%Y')
    while True:
        params = {"quantidade": 5000, "pagina": pagina, "data_ini": data_ini, "data_fim": data_fim}
        try:
            resp = requests.get(url, headers=headers, cookies=cookies, params=params, timeout=20)
            if resp.status_code != 200 or 'application/json' not in resp.headers.get('Content-Type', ''):
                st.warning(f"Erro ao buscar propostas Facta na página {pagina}.")
                break
            data = resp.json()
            propostas = data.get("propostas", [])
            propostas_fgts = [p for p in propostas if p.get("averbador") == "FGTS"]
            propostas_filtradas.extend(propostas_fgts)
            if len(propostas) < 5000:
                break
            pagina += 1
        except Exception as e:
            st.warning(f"Erro ao consultar andamento-propostas Facta (página {pagina}): {e}")
            break
    return propostas_filtradas

def main():
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")  # Atualiza a cada 2 minutos
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    start_at, end_at = get_today_range()

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

    # --- PAINEL KOLMEYA ---
    messages = obter_dados_sms()
    quantidade_sms = len(messages)
    investimento = quantidade_sms * CUSTO_POR_ENVIO
    telefones = [m.get("telefone") for m in messages if m.get("telefone")]
    cpfs = [str(m.get("cpf")).zfill(11) for m in messages if m.get("cpf")]
    telefones_limpos = set(limpar_telefone(t) for t in telefones if t)
    # Os campos abaixo são placeholders, ajuste conforme sua lógica de vendas/produção
    producao = sum(float(m.get("valor_af", 0)) for m in messages if m.get("valor_af") is not None)
    total_vendas = sum(1 for m in messages if m.get("averbador") == "FGTS")
    previsao_faturamento = 0.0
    ticket_medio = 0.0
    roi = previsao_faturamento - investimento

    st.markdown(f"""
    <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 16px; color: #fff; min-height: 100%;'>
        <h4 style='color:#fff; text-align:center;'>Kolmeya</h4>
        <div style='display: flex; justify-content: space-between; margin-bottom: 12px;'>
            <div style='text-align: center;'>
                <div style='font-size: 1.1em; color: #e0d7f7;'>Quantidade de SMS</div>
                <div style='font-size: 2em; font-weight: bold; color: #fff;'>{quantidade_sms}</div>
            </div>
            <div style='text-align: center;'>
                <div style='font-size: 1.1em; color: #e0d7f7;'>Custo por envio</div>
                <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(CUSTO_POR_ENVIO)}</div>
            </div>
        </div>
        <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>Investimento</div>
        <div style='font-size: 2em; font-weight: bold; margin-bottom: 16px; color: #fff;'>{formatar_real(investimento)}</div>
        <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
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
            <div style='display: flex; justify-content: space-between; align-items: center;'>
                <span style='color: #fff;'><b>Ticket médio</b></span>
                <span style='color: #fff;'>{formatar_real(ticket_medio)}</span>
            </div>
        </div>
        <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
        <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(roi)}</div>
    </div>
    """, unsafe_allow_html=True)

    propostas_fgts = obter_propostas_facta_ultimos_7_dias()
    st.markdown(f"""
<div style='background: #2a1a40; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
    <b>Quantidade de clientes FGTS (Facta):</b>
    <span style='font-size: 1.2em; color: #e0d7f7; font-weight: bold;'>{len(propostas_fgts)}</span>
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()