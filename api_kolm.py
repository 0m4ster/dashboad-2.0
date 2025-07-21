import os
import requests
import streamlit as st
from datetime import datetime, timedelta
import re
import pandas as pd  # Adiciona pandas para compatibilidade com exemplo
from streamlit_autorefresh import st_autorefresh
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

@st.cache_data(ttl=120)
def obter_dados_sms(start_at, end_at):
    token = os.environ.get("KOLMEYA_TOKEN")
    st.warning(f"KOLMEYA_TOKEN lido: {token}")
    st.warning(f"Período buscado: {start_at} até {end_at}")
    if not token:
        st.error("Token de autenticação não encontrado. Defina a variável de ambiente KOLMEYA_TOKEN.")
        return []
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Garantir que end_at não seja posterior a 2025-07-21 10:03
    max_date = datetime(2025, 7, 21, 10, 3)
    if end_at > max_date:
        end_at = max_date
    
    body = {
        "start_at": start_at.strftime('%Y-%m-%d %H:%M'),  # Formato Y-m-d H:i
        "end_at": end_at.strftime('%Y-%m-%d %H:%M'),      # Formato Y-m-d H:i
        "limit": 30000
    }
    try:
        resp = requests.post(API_URL, headers=headers, json=body, timeout=20)
        if resp.status_code == 422:
            st.error(f"Erro de validação na API: {resp.text}")
            return []
        resp.raise_for_status()
        messages = resp.json().get("messages", [])
        st.warning(f"Primeira mensagem recebida: {messages[0] if messages else 'Nenhuma mensagem'}")
        return messages
    except Exception as e:
        st.error(f"Erro ao buscar dados da API: {e}")
        return []

def limpar_telefone(telefone):
    if not telefone:
        return ""
    return re.sub(r"\D", "", str(telefone)) 

# Função para buscar produção na Facta

@st.cache_data(ttl=120)
def obter_producao_facta(telefones):
    url = "https://webservice.facta.com.br/proposta/andamento-propostas"
    facta_token = os.environ.get('FACTA_TOKEN', '')
    headers = {
        "Authorization": f"Bearer {facta_token}"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        propostas = data.get("propostas", [])
        # Padronize os telefones dos SMS
        telefones_limpos = set(limpar_telefone(t) for t in telefones if t)
        producao = 0.0
        total_vendas = 0
        for p in propostas:
            for campo in ["FONE", "CELULAR", "FONE2"]:
                tel_proposta = limpar_telefone(p.get(campo, ""))
                if tel_proposta and tel_proposta in telefones_limpos:
                    producao += float(p.get("valor_af", 0))
                    total_vendas += 1
                    break  # Evita contar a mesma proposta mais de uma vez
        return producao, total_vendas
    except Exception as e:
        st.error(f"Erro ao buscar dados da Facta: {e}")
        return 0.0, 0

API_URL_URA = "https://argus.app.br/apiargus/report/tabulacoesdetalhadas"

def obter_dados_ura(start_at, end_at):
    token = os.environ.get("ARGUS_TOKEN")
    if not token:
        st.error("Token de autenticação não encontrado. Defina a variável de ambiente ARGUS_TOKEN.")
        return []
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "idCampanha": 1,
        "periodoInicial": start_at.strftime('%Y-%m-%dT%H:%M:%S'),
        "periodoFinal": end_at.strftime('%Y-%m-%dT%H:%M:%S'),
    }
    try:
        resp = httpx.post(API_URL_URA, headers=headers, json=body, timeout=20, verify=False)
        resp.raise_for_status()
        data = resp.json()
        if data.get("codStatus", 0) != 1:
            st.error(f"Erro na API URA: {data.get('descStatus', 'Erro desconhecido')}")
            return []
        return data.get("tabulacoes", [])
    except Exception as e:
        st.error(f"Erro ao buscar dados da API URA (httpx): {e}")
        return []

def obter_dados_robo():
    url = "https://mr-robot-fl0t.onrender.com/total?token=meu_token_secreto"
    try:
        resp = requests.get(url, timeout=5)  # Reduzindo o timeout para 5 segundos
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectTimeout:
        st.warning("⚠️ Robô não está acessível no momento (timeout)")
        return {"cpfs_enriquecidos": 0, "cpfs_faturados": 0, "valor": 0.0}
    except requests.exceptions.ConnectionError:
        st.warning("⚠️ Não foi possível conectar ao robô (erro de conexão)")
        return {"cpfs_enriquecidos": 0, "cpfs_faturados": 0, "valor": 0.0}
    except Exception as e:
        st.error(f"⚠️ Erro ao buscar dados do robô: {str(e)}")
        return {"cpfs_enriquecidos": 0, "cpfs_faturados": 0, "valor": 0.0}

def main():
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")  # Atualiza a cada 2 minutos
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # Removido: Mostra o IP do servidor
    # get_render_ip()

    start_at, end_at = get_week_range()

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
    messages = obter_dados_sms(start_at, end_at)
    messages_fgts = [m for m in messages if str(m.get("centro_custo", "")).lower() == "fgts"]
    quantidade_sms = len(messages_fgts)
    investimento = quantidade_sms * CUSTO_POR_ENVIO
    telefones_fgts = [m.get("telefone") for m in messages_fgts if m.get("telefone")]
    producao, total_vendas = obter_producao_facta(telefones_fgts)
    previsao_faturamento = producao * 1.0
    ticket_medio = producao / total_vendas if total_vendas > 0 else 0.0
    roi = previsao_faturamento - investimento

    # --- PAINEL URA ---
    messages_ura = obter_dados_ura(start_at, end_at)
    quantidade_ura = len(messages_ura)
    investimento_ura = quantidade_ura * CUSTO_POR_LIGACAO_URA
    # Para URA, usar o campo correto de telefone
    telefones_ura = [m.get("dddTelefone") for m in messages_ura if m.get("dddTelefone")]
    producao_ura, total_vendas_ura = obter_producao_facta(telefones_ura)
    previsao_faturamento_ura = producao_ura * 1.0
    ticket_medio_ura = producao_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
    roi_ura = previsao_faturamento_ura - investimento_ura

    st.markdown("<h2 style='text-align: center;'>Métricas principais</h2>", unsafe_allow_html=True)

    # --- PAINEL ROBO ---
    dados_robo = obter_dados_robo()
    total_enriquecidos = 0
    total_faturados = 0
    total_valor = 0.0
    if isinstance(dados_robo, list):
        for item in dados_robo:
            total_enriquecidos += int(item.get('cpfs_enriquecidos', 0))
            total_faturados += int(item.get('cpfs_faturados', 0))
            valor_str = str(item.get('valor', 'R$0,00')).replace('R$', '').replace('.', '').replace(',', '.')
            try:
                total_valor += float(valor_str)
            except Exception:
                pass
    elif isinstance(dados_robo, dict):
        total_enriquecidos = int(dados_robo.get('cpfs_enriquecidos', 0))
        total_faturados = int(dados_robo.get('cpfs_faturados', 0))
        valor_str = str(dados_robo.get('valor', 'R$0,00')).replace('R$', '').replace('.', '').replace(',', '.')
        try:
            total_valor = float(valor_str)
        except Exception:
            total_valor = 0.0
    else:
        total_enriquecidos = total_faturados = 0
        total_valor = 0.0

    st.markdown(f"""
    <div style='display: flex; justify-content: center; align-items: center; width: 100%; gap: 80px; margin-bottom: 30px;'>
        <div style='text-align: center; width: 180px;'>
            <span style='font-weight: bold; font-size: 1.3em; display: block;'>CPFs enriquecidos</span>
            <span style='font-size: 2em; display: block;'>{total_enriquecidos}</span>
        </div>
        <div style='text-align: center; width: 180px;'>
            <span style='font-weight: bold; font-size: 1.3em; display: block;'>CPFs faturados</span>
            <span style='font-size: 2em; display: block;'>{total_faturados}</span>
        </div>
        <div style='text-align: center; width: 180px;'>
            <span style='font-weight: bold; font-size: 1.3em; display: block;'>Valor</span>
            <span style='font-size: 2em; display: block;'>R$ {total_valor:,.2f}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    # Remover CSS de largura mínima e ajustar para 2 colunas por painel
    col_kolmeya, col_ura = st.columns(2)

    with col_kolmeya:
        painel_kolmeya_html = f"""
        <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 16px; color: #fff; min-height: 100%;'>
            <h4 style='color:#fff; text-align:center;'>Kolmeya</h4>
            <div style='display: flex; justify-content: space-between; margin-bottom: 12px;'>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Quantidade de SMS</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{quantidade_sms}</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Custo por envio</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>R$ {CUSTO_POR_ENVIO:.2f}</div>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>Investimento</div>
            <div style='font-size: 2em; font-weight: bold; margin-bottom: 16px; color: #fff;'>R$ {investimento:,.2f}</div>
            <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Total de vendas</b></span>
                    <span style='color: #fff;'>{total_vendas}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Produção</b></span>
                    <span style='color: #fff;'>R$ {producao:,.2f}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Previsão de faturamento</b></span>
                    <span style='color: #fff;'>R$ {previsao_faturamento:,.2f}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <span style='color: #fff;'><b>Ticket médio</b></span>
                    <span style='color: #fff;'>R$ {ticket_medio:,.2f}</span>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
            <div style='font-size: 2em; font-weight: bold; color: #fff;'>R$ {roi:,.2f}</div>
        </div>
        """
        st.markdown(painel_kolmeya_html, unsafe_allow_html=True)

    with col_ura:
        painel_ura_html = f"""
        <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 16px; color: #fff; min-height: 100%;'>
            <h4 style='color:#fff; text-align:center;'>URA</h4>
            <div style='display: flex; justify-content: space-between; margin-bottom: 12px;'>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Quantidade de Ligações</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{quantidade_ura}</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 1.1em; color: #e0d7f7;'>Custo</div>
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>R$ {CUSTO_POR_LIGACAO_URA:.2f}</div>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>Investimento</div>
            <div style='font-size: 2em; font-weight: bold; margin-bottom: 16px; color: #fff;'>R$ {investimento_ura:,.2f}</div>
            <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Total de vendas</b></span>
                    <span style='color: #fff;'>{total_vendas_ura}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Produção</b></span>
                    <span style='color: #fff;'>R$ {producao_ura:,.2f}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Previsão de faturamento</b></span>
                    <span style='color: #fff;'>R$ {previsao_faturamento_ura:,.2f}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <span style='color: #fff;'><b>Ticket médio</b></span>
                    <span style='color: #fff;'>R$ {ticket_medio_ura:,.2f}</span>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>ROI</div>
            <div style='font-size: 2em; font-weight: bold; color: #fff;'>R$ {roi_ura:,.2f}</div>
        </div>
        """
        st.markdown(painel_ura_html, unsafe_allow_html=True)


if __name__ == "__main__":
    main()