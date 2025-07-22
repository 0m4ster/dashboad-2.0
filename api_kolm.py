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
    hoje = datetime.now()
    max_end_at = datetime(2025, 7, 22, 10, 24)
    if hoje > max_end_at:
        hoje = max_end_at
    start_of_week = hoje - timedelta(days=hoje.weekday())
    start_at = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_at = hoje
    all_messages = []
    periodo_max = timedelta(days=7)
    atual = start_at
    while atual < end_at:
        proximo = min(atual + periodo_max, end_at)
        body = {
            "start_at": atual.strftime('%Y-%m-%d %H:%M'),
            "end_at": proximo.strftime('%Y-%m-%d %H:%M'),
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
            if len(messages) == 30000:
                dia = atual
                while dia < proximo:
                    dia_fim = min(dia + timedelta(days=1), proximo)
                    body_dia = {
                        "start_at": dia.strftime('%Y-%m-%d %H:%M'),
                        "end_at": dia_fim.strftime('%Y-%m-%d %H:%M'),
                        "limit": 30000
                    }
                    resp_dia = requests.post(API_URL, headers=headers, json=body_dia, timeout=20)
                    if resp_dia.status_code == 422:
                        try:
                            st.error(f"Erro 422 na API Kolmeya (dia): {resp_dia.text}")
                        except Exception:
                            st.error("Erro 422 na API Kolmeya (dia) (não foi possível exibir o texto da resposta)")
                        return []
                    resp_dia.raise_for_status()
                    messages_dia = resp_dia.json().get("messages", [])
                    all_messages.extend(messages_dia)
                    dia = dia_fim
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {e}")
            return []
        atual = proximo
    return all_messages

def limpar_telefone(telefone):
    if not telefone:
        return ""
    return re.sub(r"\D", "", str(telefone)) 

# Função utilitária para formatar valores em Real

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def obter_propostas_facta_por_data(data_movimento, phpsessid=None):
    """
    Busca propostas no endpoint andamento-propostas da Facta e filtra pela data_movimento (formato DD/MM/AAAA).
    Retorna a lista de propostas filtradas.
    """
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
    params = {"quantidade": 5000}
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, params=params, timeout=20)
        if resp.status_code != 200 or 'application/json' not in resp.headers.get('Content-Type', ''):
            st.warning("Erro ao buscar propostas Facta.")
            return []
        data = resp.json()
        propostas = data.get("propostas", [])
        # Filtrar pela data_movimento
        propostas_filtradas = [p for p in propostas if p.get("data_movimento") == data_movimento]
        return propostas_filtradas
    except Exception as e:
        st.warning(f"Erro ao consultar andamento-propostas Facta: {e}")
        return []

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
    </div>
    """, unsafe_allow_html=True)

    # Coletar todas as datas (apenas a parte da data) do Kolmeya
    datas_kolmeya = set()
    centros_custo_kolmeya = set()
    for m in messages:
        enviada_em = m.get("enviada_em")
        if enviada_em:
            datas_kolmeya.add(enviada_em[:10])  # Pega só 'dd/mm/yyyy'
        centro_custo = m.get("centro_custo")
        if centro_custo:
            centros_custo_kolmeya.add(centro_custo)

    # Buscar todas as propostas da Facta (até 5000)
    propostas_facta = obter_propostas_facta_por_data(None)  # None para buscar todas
    # Filtrar propostas da Facta cujo averbador está em centros_custo_kolmeya e data_movimento presente nas datas do Kolmeya
    propostas_batidas = [
        p for p in propostas_facta
        if p.get("averbador") in centros_custo_kolmeya and p.get("data_movimento") in datas_kolmeya
    ]
    datas_batidas = set(p.get("data_movimento") for p in propostas_batidas)

    st.markdown(f"""
<div style='background: #2a1a40; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
    <b>Quantidade de propostas (Facta) com averbador presente como centro de custo no Kolmeya e datas batidas:</b>
    <span style='font-size: 1.2em; color: #e0d7f7; font-weight: bold;'>{len(propostas_batidas)}</span><br>
    <span style='font-size: 0.95em; color: #e0d7f7;'>Datas batidas: {', '.join(sorted(datas_batidas)) if datas_batidas else 'Nenhuma'}</span>
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()