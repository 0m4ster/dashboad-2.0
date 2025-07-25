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
print("OpenSSL version:", ssl.OPENSSL_VERSION)
import concurrent.futures

API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS
CUSTO_POR_LIGACAO_URA = 0.034444  # R$ 0,034444 por ligação URA

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

def obter_dados_sms(now):
    """
    Busca os dados de SMS do Kolmeya sem filtrar por datas, apenas pelo limite de registros por requisição.
    """
    token = os.environ.get("KOLMEYA_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    start_of_week = now - timedelta(days=now.weekday())
    all_messages = []

    for i in range(7):
        dia = start_of_week + timedelta(days=i)
        dia_inicio = dia.replace(hour=0, minute=0, second=0, microsecond=0)
        # Não busca dias no futuro
        if dia_inicio > now:
            break
        if i == 6:
            dia_fim = min(now - timedelta(minutes=1), dia.replace(hour=23, minute=59, second=0, microsecond=0))
        else:
            dia_fim = dia.replace(hour=23, minute=59, second=0, microsecond=0)
        if dia_fim > now:
            dia_fim = now - timedelta(minutes=1)
        bloco_inicio = dia_inicio
        while bloco_inicio < dia_fim:
            bloco_fim = min(bloco_inicio + timedelta(hours=1), dia_fim)
            # Não busca blocos no futuro
            if bloco_inicio >= now:
                break
            if bloco_fim > now:
                bloco_fim = now - timedelta(minutes=1)
                if bloco_fim <= bloco_inicio:
                    break
            body = {
                "start_at": bloco_inicio.strftime('%Y-%m-%d %H:%M'),
                "end_at": bloco_fim.strftime('%Y-%m-%d %H:%M'),
                "limit": 30000
            }
            try:
                resp = requests.post(API_URL, headers=headers, json=body, timeout=20)
                resp.raise_for_status()
                messages = resp.json().get("messages", [])
                all_messages.extend(messages)
                # Se vier exatamente 30.000, pode haver mais registros nesse intervalo, então divide em blocos menores
                if len(messages) == 30000:
                    # Divide o bloco em intervalos de 15 minutos
                    sub_inicio = bloco_inicio
                    while sub_inicio < bloco_fim:
                        sub_fim = min(sub_inicio + timedelta(minutes=15), bloco_fim)
                        if sub_inicio >= now:
                            break
                        if sub_fim > now:
                            sub_fim = now - timedelta(minutes=1)
                            if sub_fim <= sub_inicio:
                                break
                        sub_body = {
                            "start_at": sub_inicio.strftime('%Y-%m-%d %H:%M'),
                            "end_at": sub_fim.strftime('%Y-%m-%d %H:%M'),
                            "limit": 30000
                        }
                        sub_resp = requests.post(API_URL, headers=headers, json=sub_body, timeout=20)
                        sub_resp.raise_for_status()
                        sub_messages = sub_resp.json().get("messages", [])
                        all_messages.extend(sub_messages)
                        sub_inicio = sub_fim
                bloco_inicio = bloco_fim
            except Exception as e:
                bloco_inicio = bloco_fim  # Evita loop infinito em caso de erro
    return all_messages

def limpar_telefone(telefone):
    if not telefone:
        return ""
    t = re.sub(r'\D', '', str(telefone))
    # Remove zeros à esquerda
    t = t.lstrip('0')
    # Se for celular sem DDD, adiciona DDD padrão (opcional, ex: 11)
    if len(t) == 8 or len(t) == 9:
        t = '11' + t  # Ajuste conforme seu DDD padrão
    # Se for telefone fixo sem DDD (8 dígitos), adiciona DDD padrão
    if len(t) == 10 or len(t) == 11:
        return t[-11:]  # Mantém apenas os 11 últimos dígitos (DDD+celular)
    return t

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
    """
    Consulta o endpoint de resumo dos jobs enviados por um período específico (formato Y-m).
    """
    token = os.environ.get("KOLMEYA_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/quantity-jobs"
    if periodo is None:
        periodo = datetime.now().strftime('%Y-%m')
    body = {"period": periodo}
    try:
        resp = requests.post(API_URL, headers=headers, json=body, timeout=20)
        print("Status:", resp.status_code)
        print("Resposta:", resp.text)
        resp.raise_for_status()
        return resp.json().get("jobs", [])
    except Exception as e:
        print("Erro ao consultar jobs:", e)
        return []

@st.cache_data(ttl=600)
def ler_base(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        try:
            return pd.read_csv(uploaded_file, dtype=str, sep=';')
        except Exception:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, dtype=str, sep=',')
    else:
        return pd.read_excel(uploaded_file, dtype=str)

# Função para consultar o resumo dos jobs do Kolmeya

def obter_resumo_jobs_kolmeya(period, token=None):
    """
    Consulta o endpoint de resumo dos jobs enviados por período (YYYY-MM) do Kolmeya.
    """
    import requests
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/quantity-jobs"
    if token is None:
        token = os.environ.get("KOLMEYA_TOKEN", "")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    body = {"period": period}
    try:
        resp = requests.post(API_URL, headers=headers, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"erro": str(e)}

def main():
    fixed_now = datetime.now()
    leads_gerados = st.session_state.get('leads_gerados', 0)  # Pega valor salvo, padrão 0
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")  # Atualiza a cada 2 minutos
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    start_at, end_at = get_today_range(fixed_now)

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

    col1, col2 = st.columns(2)
    # Garante que os valores de produção e vendas da Facta estejam sempre disponíveis no session_state
    if "producao_facta" not in st.session_state:
        st.session_state["producao_facta"] = 0.0
    if "total_vendas_facta" not in st.session_state:
        st.session_state["total_vendas_facta"] = 0
    with col1:
        # --- PAINEL KOLMEYA ---
        messages = obter_dados_sms(fixed_now)
        quantidade_sms = len(messages)
        investimento = quantidade_sms * CUSTO_POR_ENVIO
        telefones = []
        for m in messages:
            tel = m.get('telefone') if isinstance(m, dict) else None
            if tel:
                telefones.append(limpar_telefone(tel))
        cpfs = [str(m.get("cpf")).zfill(11) for m in messages if isinstance(m, dict) and m.get("cpf")]
        # Produção e vendas vindos da Facta, se já consultados
        producao = st.session_state["producao_facta"]
        total_vendas = st.session_state["total_vendas_facta"]
        # Previsão de faturamento = produção * 0,171
        previsao_faturamento = producao * 0.171
        # Ticket médio = produção / total de vendas
        ticket_medio = producao / total_vendas if total_vendas > 0 else 0.0
        roi = previsao_faturamento - investimento

        # --- RESUMO DOS JOBS ---
        st.markdown("<h4 style='color:#fff; text-align:center;'>Resumo dos Jobs (Mês Atual)</h4>", unsafe_allow_html=True)
        periodo_atual = datetime.now().strftime('%Y-%m')
        resumo_jobs = obter_resumo_jobs(periodo=periodo_atual)
        if resumo_jobs:
            st.dataframe(pd.DataFrame(resumo_jobs))
        else:
            st.info("Nenhum job encontrado para o mês atual.")

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
                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                    <span style='color: #fff;'><b>Ticket médio</b></span>
                    <span style='color: #fff;'>{formatar_real(ticket_medio)}</span>
                </div>
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <span style='color: #fff;'><b>Leads gerados</b></span>
                    <span style='color: #fff;'>{leads_gerados}</span>
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
        periodoInicial = (fixed_now - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00')
        periodoFinal = fixed_now.strftime('%Y-%m-%dT23:59:59')
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

    # --- UPLOAD DE BASE LOCAL ---
    uploaded_file = st.file_uploader("Faça upload da base de CPFs/Telefones (Excel ou CSV)", type=["csv", "xlsx"])
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            st.markdown("<b>Base carregada:</b>", unsafe_allow_html=True)
            st.dataframe(df_base)
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}. Tente salvar o arquivo como CSV separado por ponto e vírgula (;) ou Excel.")

    # --- RESUMO DOS JOBS KOLMEYA ---
    st.markdown("<h3>Resumo dos Jobs Kolmeya</h3>", unsafe_allow_html=True)
    col_period, col_token = st.columns([1,2])
    with col_period:
        period = st.text_input("Período (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
    with col_token:
        token_input = st.text_input("Token Kolmeya (opcional, usa env se vazio)", value="", type="password")
    if st.button("Consultar resumo dos jobs Kolmeya"):
        resultado_jobs = obter_resumo_jobs_kolmeya(period, token=token_input or None)
        leads_gerados = 0
        if isinstance(resultado_jobs, dict) and "erro" in resultado_jobs:
            st.error(f"Erro ao consultar jobs: {resultado_jobs['erro']}")
        elif isinstance(resultado_jobs, list) and resultado_jobs:
            df_jobs = pd.DataFrame(resultado_jobs)
            if 'centro_custo' in df_jobs.columns:
                df_jobs = df_jobs[df_jobs['centro_custo'] == 'FGTS']
            st.dataframe(df_jobs)
            if 'acessos' in df_jobs.columns:
                try:
                    leads_gerados = df_jobs['acessos'].astype(int).sum()
                except Exception:
                    leads_gerados = 0
        elif isinstance(resultado_jobs, dict) and resultado_jobs:
            df_jobs = pd.DataFrame([resultado_jobs])
            if 'centro_custo' in df_jobs.columns:
                df_jobs = df_jobs[df_jobs['centro_custo'] == 'FGTS']
            st.dataframe(df_jobs)
            if 'acessos' in df_jobs.columns:
                try:
                    leads_gerados = df_jobs['acessos'].astype(int).sum()
                except Exception:
                    leads_gerados = 0
        else:
            st.info("Nenhum dado retornado para o período informado.")
        st.session_state['leads_gerados'] = leads_gerados  # Salva para uso no painel Kolmeya
    # Exibir leads_gerados no painel Kolmeya
    # (Abaixo, ajuste a linha do painel Kolmeya para usar a variável leads_gerados)

    # Libera memória dos DataFrames grandes após uso
    gc.collect()

if __name__ == "__main__":
    main()