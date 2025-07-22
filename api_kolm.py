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
    # Calcular segunda-feira desta semana
    hoje = datetime.now()
    # Definir a data máxima permitida (ajuste aqui se necessário)
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

# Função para buscar produção na Facta

@st.cache_data(ttl=120)
def obter_producao_facta(telefones):
    import re
    url = "https://webservice-homol.facta.com.br/proposta/consulta-cliente"
    facta_token = os.environ.get('FACTA_TOKEN', '')
    headers = {
        "Authorization": f"Bearer {facta_token}"
    }
    params = {
        "quantidade": 5000,
        "pagina": 1,
        "convenio": 3,
        "consulta_sub": "N"
    }
    all_propostas = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        print("Status code:", resp.status_code)
        print("Headers:", resp.headers)
        print("Resposta bruta:", resp.text)
        if 'application/json' in resp.headers.get('Content-Type', ''):
            try:
                data = resp.json()
            except Exception as e:
                st.error(f"Erro ao decodificar resposta JSON da API Facta: {e}")
                st.error(f"Resposta bruta: {resp.text}")
                return [], 0, 0.0
        else:
            st.error("A resposta da API Facta não é JSON. Veja o conteúdo retornado no log.")
            st.error(f"Resposta bruta: {resp.text}")
            return [], 0, 0.0
        propostas = data.get("propostas", [])
        if not propostas:
            break
        all_propostas.extend(propostas)
        if len(propostas) < 5000:
            break
        params["pagina"] += 1
    def limpar_telefone(telefone):
        if not telefone:
            return ""
        return re.sub(r"\D", "", str(telefone))
    telefones_limpos = set(limpar_telefone(t) for t in telefones if t)
    telefones_batidos = set()
    for p in all_propostas:
        telefones_proposta = [
            limpar_telefone(p.get("FONE", "")),
            limpar_telefone(p.get("CELULAR", "")),
            limpar_telefone(p.get("FONE2", ""))
        ]
        for tel in telefones_proposta:
            if tel and tel in telefones_limpos:
                telefones_batidos.add(tel)
                break
    producao = 0.0
    for p in all_propostas:
        telefones_proposta = [
            limpar_telefone(p.get("FONE", "")),
            limpar_telefone(p.get("CELULAR", "")),
            limpar_telefone(p.get("FONE2", ""))
        ]
        if any(tel and tel in telefones_batidos for tel in telefones_proposta):
            try:
                producao += float(p.get("valor_af", 0))
            except Exception:
                pass
    return list(telefones_batidos), len(telefones_batidos), producao

def obter_telefones_facta_por_cpf(cpfs, ambiente="producao", phpsessid=None):
    """Busca todos os telefones (FONE, CELULAR, FONE2, FONERECADO) de cada CPF na Facta e retorna o set de telefones encontrados."""
    import requests
    import os
    import re
    facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    if ambiente == "homologacao":
        url_base = "https://webservice-homol.facta.com.br/proposta/consulta-cliente?cpf="
    else:
        url_base = "https://webservice.facta.com.br/proposta/consulta-cliente?cpf="
    headers = {
        "Authorization": f"Bearer {facta_token}"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None
    telefones_facta = set()
    for cpf in cpfs:
        url = f"{url_base}{cpf}"
        try:
            resp = requests.get(url, headers=headers, cookies=cookies, timeout=10)
            if resp.status_code != 200:
                st.warning(f"Facta: resposta {resp.status_code} para CPF {cpf}")
                continue
            if 'application/json' not in resp.headers.get('Content-Type', ''):
                st.warning(f"Facta: resposta não JSON para CPF {cpf}")
                continue
            data = resp.json()
            if data.get("erro") is True:
                st.warning(f"Facta: erro True para CPF {cpf}")
                continue
            clientes = data.get("cliente", [])
            for c in clientes:
                for campo in ["FONE", "CELULAR", "FONE2", "FONERECADO"]:
                    tel = re.sub(r"\D", "", str(c.get(campo, "")))
                    if tel:
                        telefones_facta.add(tel)
        except Exception as e:
            st.warning(f"Erro ao consultar Facta para CPF {cpf}: {e}")
            continue
    return telefones_facta

def obter_andamento_propostas_facta(params=None, ambiente="producao", phpsessid=None):
    """Consulta o endpoint andamento-propostas da Facta com os parâmetros fornecidos."""
    import requests
    import os
    facta_token = os.environ.get('FACTA_TOKEN', '')
    if phpsessid is None:
        phpsessid = os.environ.get('FACTA_PHPSESSID', None)
    if ambiente == "homologacao":
        url_base = "https://webservice-homol.facta.com.br/proposta/andamento-propostas"
    else:
        url_base = "https://webservice.facta.com.br/proposta/andamento-propostas"
    headers = {
        "Authorization": f"Bearer {facta_token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    cookies = {"PHPSESSID": phpsessid} if phpsessid else None
    try:
        resp = requests.get(url_base, headers=headers, cookies=cookies, params=params, timeout=20)
        if resp.status_code != 200:
            st.warning(f"Facta andamento-propostas: resposta {resp.status_code}")
            return []
        if 'application/json' not in resp.headers.get('Content-Type', ''):
            st.warning("Facta andamento-propostas: resposta não JSON")
            return []
        data = resp.json()
        if data.get("erro") is True:
            st.warning(f"Facta andamento-propostas: erro True. Mensagem: {data.get('mensagem')}")
            return []
        return data.get("propostas", [])
    except Exception as e:
        st.warning(f"Erro ao consultar andamento-propostas Facta: {e}")
        return []

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
    except httpx.HTTPError as e:
        if 'SSLV3_ALERT_HANDSHAKE_FAILURE' in str(e):
            st.error("Erro ao buscar dados da API URA: Falha de handshake SSL. Verifique o certificado ou tente novamente mais tarde.")
        else:
            st.error(f"Erro ao buscar dados da API URA (httpx): {e}")
        return []
    except Exception as e:
        st.error(f"Erro ao buscar dados da API URA (httpx): {e}")
        return []

def obter_dados_robo():
    url = "https://mr-robot-flot.onrender.com/api/total?token=12345"
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

# Função utilitária para formatar valores em Real

def formatar_real(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def main():
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")  # Atualiza a cada 2 minutos
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # Removido: Mostra o IP do servidor
    # get_render_ip()

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
    # start_at, end_at = get_week_range()  # Alterado para pegar a semana a partir de segunda-feira
    messages = obter_dados_sms()
    quantidade_sms = len(messages)
    investimento = quantidade_sms * CUSTO_POR_ENVIO
    telefones = [m.get("telefone") for m in messages if m.get("telefone")]
    cpfs = [str(m.get("cpf")).zfill(11) for m in messages if m.get("cpf")]
    # Buscar todos os telefones dos clientes na Facta
    telefones_facta = obter_telefones_facta_por_cpf(cpfs)
    # Comparar com os telefones do Kolmeya
    telefones_limpos = set(limpar_telefone(t) for t in telefones if t)
    telefones_batidos = telefones_limpos & telefones_facta
    total_vendas = len(telefones_batidos)
    producao = 0.0  # Não temos valor_af nesse endpoint, só se buscar propostas por CPF
    previsao_faturamento = float(producao) * 1.0
    ticket_medio = float(producao) / total_vendas if total_vendas > 0 else 0.0
    roi = previsao_faturamento - investimento

    # Exibir dados da Facta no painel Kolmeya
    st.markdown("""
    <div style='background: #2a1a40; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
        <b>Telefones vindos da Facta:</b><br>
        <span style='font-size: 0.95em; color: #e0d7f7;'>Quantidade: {}</span><br>
        <span style='font-size: 0.95em; color: #e0d7f7;'>{}</span>
    </div>
    <div style='background: #1a1a2a; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
        <b>Telefones batidos (Kolmeya ∩ Facta):</b><br>
        <span style='font-size: 0.95em; color: #e0d7f7;'>Quantidade: {}</span><br>
        <span style='font-size: 0.95em; color: #e0d7f7;'>{}</span>
    </div>
    """.format(
        len(telefones_facta),
        ', '.join(sorted(telefones_facta)) if telefones_facta else 'Nenhum',
        len(telefones_batidos),
        ', '.join(sorted(telefones_batidos)) if telefones_batidos else 'Nenhum'
    ), unsafe_allow_html=True)

    # Comparação de datas e centro de custo Kolmeya x Facta (andamento-propostas)
    datas_kolmeya = set(m.get('data_envio') for m in messages if m.get('data_envio'))
    centro_custo_kolmeya = set(m.get('centro_custo') for m in messages if m.get('centro_custo'))
    params = {"convenio": 3, "quantidade": 5000}
    propostas_facta = obter_andamento_propostas_facta(params)
    propostas_batidas = [p for p in propostas_facta if p.get('data_movimento') in datas_kolmeya and p.get('centro_custo') in centro_custo_kolmeya]
    st.markdown(f"""
    <div style='background: #1a2a1a; border-radius: 10px; padding: 12px; margin-bottom: 16px;'>
        <b>Propostas batidas Kolmeya x Facta (por data e centro de custo):</b><br>
        <span style='font-size: 0.95em; color: #e0d7f7;'>Quantidade: {len(propostas_batidas)}</span>
    </div>
    """, unsafe_allow_html=True)

    # --- PAINEL URA ---
    messages_ura = obter_dados_ura(start_at, end_at)
    quantidade_ura = len(messages_ura)
    investimento_ura = quantidade_ura * CUSTO_POR_LIGACAO_URA
    # Para URA, usar o campo correto de telefone
    telefones_ura = [m.get("dddTelefone") for m in messages_ura if m.get("dddTelefone")]
    producao_ura, total_vendas_ura, _ = obter_producao_facta(telefones_ura)
    try:
        producao_ura = float(producao_ura)
    except Exception:
        producao_ura = 0.0
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
                    <div style='font-size: 2em; font-weight: bold; color: #fff;'>{formatar_real(CUSTO_POR_ENVIO)}</div>
                </div>
            </div>
            <div style='font-size: 1.1em; margin-bottom: 8px; color: #e0d7f7;'>Investimento</div>
            <div style='font-size: 2em; font-weight: bold; margin-bottom: 16px; color: #fff;'>{formatar_real(investimento)}</div>
            <div style='background-color: rgba(30, 20, 50, 0.95); border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); padding: 18px 24px; margin-bottom: 16px;'>
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Total de vendas</b></span>
                     <span style='color: #fff;'>{', '.join(map(str, telefones_batidos)) if telefones_batidos else '0'}</span>
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
        """
        st.markdown(painel_ura_html, unsafe_allow_html=True)


if __name__ == "__main__":
    main()