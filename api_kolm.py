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

def obter_dados_sms():
    """
    Busca os dados de SMS do Kolmeya, respeitando o limite de 7 dias e garantindo que end_at nunca seja maior que o horário atual do servidor Kolmeya.
    """
    token = os.environ.get("KOLMEYA_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    now = datetime.now()
    # end_at nunca pode ser maior que o horário atual
    end_at = now - timedelta(minutes=1)
    # start_at deve ser no máximo 7 dias antes de end_at
    start_at = end_at - timedelta(days=6)
    start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
    body = {
        "start_at": start_at.strftime('%Y-%m-%d %H:%M'),
        "end_at": end_at.strftime('%Y-%m-%d %H:%M'),
        "limit": 30000
    }
    all_messages = []
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
        st.error(f"Erro ao buscar dados da API Kolmeya: {e}")
        return []
    st.write("Mensagens mais recentes do Kolmeya:", all_messages)
    return all_messages

def limpar_telefone(telefone):
    if not telefone:
        return ""
    return re.sub(r"\D", "", str(telefone)) 

# Função utilitária para formatar valores em Real

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def obter_clientes_facta_por_cpfs(cpfs, phpsessid=None):
    """
    Consulta o endpoint consulta-cliente da Facta para cada CPF fornecido e retorna os dados dos clientes.
    """
    facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    facta_env = os.environ.get('FACTA_ENV', 'prod').lower()
    if facta_env == 'homolog':
        url_base = "https://webservice-homol.facta.com.br/proposta/consulta-cliente"
    else:
        url_base = "https://webservice.facta.com.br/proposta/consulta-cliente"
    headers = {
        "Authorization": f"Bearer {facta_token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None
    st.write("CPFs enviados para consulta na Facta:", cpfs)  # Debug: mostra CPFs enviados
    clientes = []
    respostas_debug = []  # Lista para armazenar debug de cada resposta
    for cpf in set(cpfs):
        params = {"cpf": cpf}
        try:
            resp = requests.get(url_base, headers=headers, cookies=cookies, params=params, timeout=20)
            resposta_debug = {
                "cpf": cpf,
                "status_code": resp.status_code,
                "raw_response": resp.text
            }
            try:
                resposta_debug["json"] = resp.json()
            except Exception as e_json:
                resposta_debug["json_error"] = str(e_json)
            respostas_debug.append(resposta_debug)
            if resp.status_code != 200 or 'application/json' not in resp.headers.get('Content-Type', ''):
                continue
            data = resp.json()
            if not data.get("erro") and data.get("cliente"):
                clientes.extend(data["cliente"])
        except Exception as e:
            respostas_debug.append({
                "cpf": cpf,
                "erro": str(e)
            })
    st.write("Debug detalhado das respostas da API Facta (consulta-cliente):", respostas_debug)
    return clientes

def buscar_clientes_facta_e_comparar_telefones(telefones, phpsessid=None):
    """
    Busca todos os clientes da Facta (ou de uma base local, se disponível) e compara os telefones extraídos dos SMS
    com os campos FONE, FONE2 e CELULAR de cada cliente. Retorna os clientes que possuem algum telefone igual.
    """
    facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    facta_env = os.environ.get('FACTA_ENV', 'prod').lower()
    if facta_env == 'homolog':
        url_base = "https://webservice-homol.facta.com.br/proposta/consulta-clientes"
    else:
        url_base = "https://webservice.facta.com.br/proposta/consulta-clientes"
    headers = {
        "Authorization": f"Bearer {facta_token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None
    # st.write("Telefones extraídos dos SMS:", telefones)  # Removido para evitar duplicidade
    clientes = []
    respostas_debug = []
    try:
        # Busca todos os clientes (ajuste o endpoint conforme a documentação da Facta)
        resp = requests.get(url_base, headers=headers, cookies=cookies, timeout=60)
        resposta_debug = {
            "status_code": resp.status_code,
            "raw_response": resp.text
        }
        try:
            resposta_debug["json"] = resp.json()
        except Exception as e_json:
            resposta_debug["json_error"] = str(e_json)
        respostas_debug.append(resposta_debug)
        if resp.status_code == 200 and 'application/json' in resp.headers.get('Content-Type', ''):
            data = resp.json()
            todos_clientes = data.get("clientes", []) if "clientes" in data else data.get("cliente", [])
            # Normaliza os telefones dos clientes e compara
            telefones_set = set(telefones)
            for cliente in todos_clientes:
                for campo in ["FONE", "FONE2", "CELULAR"]:
                    tel_cliente = limpar_telefone(cliente.get(campo, ""))
                    if tel_cliente and tel_cliente in telefones_set:
                        clientes.append(cliente)
                        break
    except Exception as e:
        respostas_debug.append({"erro": str(e)})
    st.write("Debug detalhado da busca e comparação de clientes da Facta:", respostas_debug)
    return clientes

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
    telefones = [limpar_telefone(m.get("telefone")) for m in messages if m.get("telefone")]
    st.write("Telefones extraídos dos SMS:", telefones)
    cpfs = [str(m.get("cpf")).zfill(11) for m in messages if m.get("cpf")]
    st.write("CPFs extraídos dos SMS:", cpfs)
    # Os campos abaixo são placeholders, ajuste conforme sua lógica de vendas/produção
    producao = sum(
        float(m.get("valor_af", 0))
        for m in messages
        if m.get("averbador", "").strip().upper() == "FGTS" and m.get("valor_af") is not None
    )
    total_vendas = sum(
        1 for m in messages
        if m.get("averbador", "").strip().upper() == "FGTS"
    )
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

    if cpfs:
        clientes_facta = obter_clientes_facta_por_cpfs(cpfs)
        st.markdown(f"""
    <div style='background: #2a1a40; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
        <b>Quantidade de clientes FGTS (Facta):</b>
        <span style='font-size: 1.2em; color: #e0d7f7; font-weight: bold;'>{len(clientes_facta)}</span><br>
    </div>
    """, unsafe_allow_html=True)
    else:
        st.warning("Nenhum CPF foi extraído dos SMS. Não é possível consultar clientes na Facta sem CPF.")

    if telefones:
        clientes_facta = buscar_clientes_facta_e_comparar_telefones(telefones)
        st.markdown(f"""
    <div style='background: #2a1a40; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
        <b>Quantidade de clientes Facta com telefone encontrado nos SMS:</b>
        <span style='font-size: 1.2em; color: #e0d7f7; font-weight: bold;'>{len(clientes_facta)}</span><br>
    </div>
    """, unsafe_allow_html=True)
    else:
        st.warning("Nenhum telefone foi extraído dos SMS. Não é possível comparar com clientes da Facta sem telefone.")

if __name__ == "__main__":
    main()