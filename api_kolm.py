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
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import urllib3

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

def obter_dados_sms_periodo(data_ini, data_fim):
    """
    Busca os dados de SMS do Kolmeya para o período de data_ini até data_fim (inclusive).
    """
    token = os.environ.get("KOLMEYA_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    API_URL = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    all_messages = []
    dia = data_ini
    while dia <= data_fim:
        dia_inicio = datetime.combine(dia, datetime.min.time())
        dia_fim = datetime.combine(dia, datetime.max.time())
        bloco_inicio = dia_inicio
        while bloco_inicio < dia_fim:
            bloco_fim = min(bloco_inicio + timedelta(hours=1), dia_fim)
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
                if len(messages) == 30000:
                    # Divide o bloco em intervalos de 15 minutos
                    sub_inicio = bloco_inicio
                    while sub_inicio < bloco_fim:
                        sub_fim = min(sub_inicio + timedelta(minutes=15), bloco_fim)
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
        dia += timedelta(days=1)
    return all_messages

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
    Consulta o endpoint de ligações detalhadas da URA (Argus) e retorna todas as ligações.
    O filtro por período será feito no main(), como é feito com SMS.
    """
    argus_token = os.environ.get('ARGUS_TOKEN', '')
    
    # Debug do token
    print(f"=== DEBUG TOKEN URA ===")
    print(f"Token length: {len(argus_token)}")
    print(f"Token starts with: {argus_token[:10] if argus_token else 'None'}...")
    
    # Verifica se o token está vazio
    if not argus_token:
        print("⚠️ AVISO: ARGUS_TOKEN não está configurado!")
        print("Configure a variável de ambiente ARGUS_TOKEN")
    else:
        print("✅ Token configurado")
    
    print(f"=======================")
    
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
    
    # Debug do body
    print(f"=== DEBUG BODY URA ===")
    print(f"Body enviado: {body}")
    print(f"=====================")
    
    if idTabulacao is not None:
        body["idTabulacao"] = idTabulacao
    if idGrupoUsuario is not None:
        body["idGrupoUsuario"] = idGrupoUsuario
    if idUsuario is not None:
        body["idUsuario"] = idUsuario
    if idLote is not None:
        body["idLote"] = idLote
    
    # Configurações para contornar problemas de SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        print(f"=== DEBUG REQUEST URA ===")
        print(f"URL: {url}")
        print(f"Headers: {headers}")
        print(f"Body: {body}")
        
        # Tenta com verify=False para contornar problemas de SSL
        resp = requests.post(url, headers=headers, json=body, timeout=30, verify=False)
        print(f"Status Code: {resp.status_code}")
        print(f"Response Headers: {dict(resp.headers)}")
        
        resp.raise_for_status()
        data = resp.json()
        
        print(f"Response Data Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        print(f"Response Data Type: {type(data)}")
        print(f"========================")
        
        # Retorna todas as tabulações sem filtrar por período (como é feito com SMS)
        # Tenta diferentes chaves possíveis na resposta
        tabulacoes = data.get("tabulacoes", [])
        if not tabulacoes:
            tabulacoes = data.get("ligacoes", [])
        if not tabulacoes:
            tabulacoes = data.get("dados", [])
        if not tabulacoes:
            # Se não encontrar nenhuma chave específica, usa toda a resposta
            if isinstance(data, list):
                tabulacoes = data
            else:
                tabulacoes = []
        
        # Log para debug da API
        print(f"=== DEBUG API URA ===")
        print(f"Status da API: {data.get('codStatus', 'N/A')} - {data.get('descStatus', 'N/A')}")
        print(f"Qtde Registros na API: {data.get('qtdeRegistros', 'N/A')}")
        print(f"Total de registros retornados pela API: {len(tabulacoes)}")
        print(f"Tipo da resposta: {type(data)}")
        print(f"Chaves disponíveis: {list(data.keys()) if isinstance(data, dict) else 'Não é dict'}")
        
        # Mostra estrutura dos dados se não há registros
        if not tabulacoes:
            print("Estrutura completa da resposta da API:")
            if isinstance(data, dict):
                for key, value in data.items():
                    print(f"  {key}: {value}")
            else:
                print(f"  Resposta: {data}")
        
        # Mostra exemplos de registros se existirem
        if tabulacoes:
            print("Exemplos de registros retornados:")
            for i, registro in enumerate(tabulacoes[:2]):  # Mostra as 2 primeiras
                if isinstance(registro, dict):
                    print(f"  Registro {i+1}:")
                    for key, value in registro.items():
                        print(f"    {key}: {value}")
        
        print(f"=====================")
        
        # Retorna dados estruturados para o dashboard
        return {
            "codStatus": data.get("codStatus", 0),
            "descStatus": data.get("descStatus", ""),
            "qtdeRegistros": len(tabulacoes),
            "ligacoes": tabulacoes,  # Usa registros como ligações
            "quantidade_ura": len(tabulacoes),  # Será filtrado no main()
            "custo_por_ligacao": 0.034444,
            "investimento": len(tabulacoes) * 0.034444,  # Será recalculado no main()
            "atendidas": 0,  # Será calculado no main()
            "total_vendas": 0,  # Será calculado no main()
            "producao": 0.0,  # Será calculado no main()
            "previsao_faturamento": 0.0,  # Será calculado no main()
            "ticket_medio": 0.0,  # Será calculado no main()
            "roi": 0.0,  # Será calculado no main()
            "percentual_atendem": 0.0,  # Será calculado no main()
            "leads_gerados": 0,  # Será calculado no main()
            "percentual_conversao_lead": 0.0,  # Será calculado no main()
            "ligacoes_por_lead": 0.0,  # Será calculado no main()
            "percentual_leads_converte_vendas": 0.0,  # Será calculado no main()
            "ligacoes_por_venda": 0.0,  # Será calculado no main()
            "custo_por_lead": 0.0,  # Será calculado no main()
            "custo_por_venda": 0.0,  # Será calculado no main()
            "faturamento_medio_por_venda": 0.0  # Será calculado no main()
        }
        
    except Exception as e:
        print(f"=== ERRO URA ===")
        print(f"Erro completo: {e}")
        print(f"Tipo do erro: {type(e)}")
        print(f"=================")
        
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

def obter_resumo_jobs(periodo=None):
    """
    Consulta o endpoint de resumo dos jobs enviados por um período específico (formato Y-m).
    Retorna os jobs e calcula o total de acessos (leads gerados).
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
        jobs = resp.json().get("jobs", [])
        
        # Calcula o total de acessos (leads gerados)
        total_acessos = 0
        for job in jobs:
            if isinstance(job, dict) and 'acessos' in job:
                try:
                    acessos = int(job['acessos']) if job['acessos'] and str(job['acessos']).isdigit() else 0
                    total_acessos += acessos
                except (ValueError, TypeError):
                    continue
        
        # Armazena o total de leads gerados no session_state
        if 'st' in globals() or 'streamlit' in globals():
            try:
                import streamlit as st
                st.session_state['leads_gerados'] = total_acessos
            except:
                pass
        
        return jobs
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
    """
    Consulta o endpoint de resumo dos jobs enviados por período (YYYY-MM) do Kolmeya.
    Retorna os jobs e calcula o total de acessos (leads gerados) e entregues.
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
        data = resp.json()
        jobs = data.get("jobs", [])
        # Filtra apenas jobs com centro_custo FGTS
        jobs_fgts = [job for job in jobs if isinstance(job, dict) and str(job.get('centro_custo', '')).strip().lower() == 'fgts']
        # Calcula o total de acessos (leads gerados)
        total_acessos = 0
        total_entregues = 0
        for job in jobs_fgts:
            if 'acessos' in job:
                try:
                    acessos = int(job['acessos']) if job['acessos'] and str(job['acessos']).isdigit() else 0
                    total_acessos += acessos
                except (ValueError, TypeError):
                    continue
            if 'entregues' in job:
                try:
                    entregues = int(job['entregues']) if job['entregues'] and str(job['entregues']).isdigit() else 0
                    total_entregues += entregues
                except (ValueError, TypeError):
                    continue
        # Adiciona os totais ao retorno
        data['jobs'] = jobs_fgts
        data['total_leads_gerados'] = total_acessos
        data['total_entregues'] = total_entregues
        
        # Armazena os totais no session_state se estiver no contexto do Streamlit
        if 'st' in globals() or 'streamlit' in globals():
            try:
                import streamlit as st
                st.session_state['leads_gerados'] = total_acessos
                st.session_state['total_entregues'] = total_entregues
            except:
                pass
        
        return data
    except Exception as e:
        return {"erro": str(e)}

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

    start_at, end_at = get_today_range(datetime.combine(data_ini, datetime.min.time()))

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
    
    # Consulta os jobs FGTS para o período selecionado
    meses = set()
    atual = data_ini.replace(day=1)
    while atual <= data_fim:
        meses.add(atual.strftime('%Y-%m'))
        if atual.month == 12:
            atual = atual.replace(year=atual.year+1, month=1)
        else:
            atual = atual.replace(month=atual.month+1)
    total_acessos_fgts = 0
    for mes in meses:
        resumo_jobs = obter_resumo_jobs_kolmeya(mes)
        jobs = resumo_jobs.get('jobs', [])
        for job in jobs:
            if isinstance(job, dict) and 'fgts' in str(job.get('centro_custo', '')).lower():
                try:
                    acessos = int(job['acessos']) if job['acessos'] and str(job['acessos']).isdigit() else 0
                except (ValueError, TypeError):
                    acessos = 0
                total_acessos_fgts += acessos
    st.session_state['acessos'] = total_acessos_fgts
    # Soma dos entregues dos jobs FGTS
    sms_entregues_jobs = 0
    for mes in meses:
        resumo_jobs = obter_resumo_jobs_kolmeya(mes)
        jobs = resumo_jobs.get('jobs', [])
        for job in jobs:
            if 'entregues' in job:
                try:
                    entregues = int(job['entregues']) if job['entregues'] and str(job['entregues']).isdigit() else 0
                except (ValueError, TypeError):
                    entregues = 0
                sms_entregues_jobs += entregues
    # No painel Kolmeya, use sms_entregues_jobs para exibir SMS entregues:
    total_entregues = sms_entregues_jobs

    with col1:
        # --- PAINEL KOLMEYA ---
        # Obtenha todas as mensagens
        messages_all = obter_dados_sms_periodo(data_ini, data_fim)
        # Filtra apenas mensagens com centro_custo FGTS (contendo 'fgts' em qualquer parte)
        messages_fgts = [m for m in messages_all if isinstance(m, dict) and 'fgts' in str(m.get('centro_custo', '')).lower()]
        quantidade_sms = len(messages_fgts)
        investimento = quantidade_sms * CUSTO_POR_ENVIO
        telefones = []
        for m in messages_fgts:
            tel = m.get('telefone') if isinstance(m, dict) else None
            if tel:
                telefones.append(limpar_telefone(tel))
        cpf = [str(m.get("cpf")).zfill(11) for m in messages_fgts if isinstance(m, dict) and m.get("cpf")]
        # --- NOVOS NA CARTEIRA ---
        # Buscar CPFs da semana anterior
        semana_atual_ini = datetime.combine(data_ini, datetime.min.time()) - timedelta(days=datetime.combine(data_ini, datetime.min.time()).weekday())
        semana_anterior_ini = semana_atual_ini - timedelta(days=7)
        semana_anterior_fim = semana_atual_ini - timedelta(seconds=1)
        # Busca SMS da semana anterior (todas as mensagens)
        messages_semana_anterior = obter_dados_sms(semana_anterior_fim)
        cpfs_semana_anterior = set(str(m.get("cpf")).zfill(11) for m in messages_semana_anterior if isinstance(m, dict) and m.get("cpf"))
        cpfs_semana_atual = set(str(m.get("cpf")).zfill(11) for m in messages_all if isinstance(m, dict) and m.get("cpf"))
        novos_na_carteira = len(cpfs_semana_atual - cpfs_semana_anterior)
        # Produção e vendas vindos da Facta, se já consultados
        producao = st.session_state["producao_facta"]
        total_vendas = st.session_state["total_vendas_facta"]
        # Obtém leads_gerados antes de usar nas cálculos
        leads_gerados = st.session_state.get('acessos', 0)
        # Previsão de faturamento = produção * 0,171
        previsao_faturamento = producao * 0.171
        # Ticket médio = produção / total de vendas
        ticket_medio = producao / total_vendas if total_vendas > 0 else 0.0
        # Faturamento médio por venda = previsão de faturamento / total de vendas
        faturamento_medio_por_venda = previsao_faturamento / total_vendas if total_vendas > 0 else 0.0
        # Custo por lead = investimento / leads gerados
        custo_por_lead = investimento / novos_na_carteira if novos_na_carteira > 0 else 0.0
        # Disparos p/ uma venda = quantidade de SMS / total de vendas
        disparos_por_venda = quantidade_sms / total_vendas if total_vendas > 0 else 0.0
        # % p/ venda = total de venda / quantidade de sms
        percentual_por_venda = (total_vendas / quantidade_sms * 100) if quantidade_sms > 0 else 0.0
        # Disparos p/ um lead = quantidade de sms / por leads gerados
        disparos_por_lead = quantidade_sms / novos_na_carteira if novos_na_carteira > 0 else 0.0
        # Leads p/ venda = leads gerados / total vendas
        leads_por_venda = novos_na_carteira / total_vendas if total_vendas > 0 else 0.0
        roi = previsao_faturamento - investimento
        total_entregues = st.session_state.get('total_entregues', 0)
        # Calcula a porcentagem de interação (leads gerados em relação ao total de SMS)
        interacao_percentual = (novos_na_carteira / quantidade_sms * 100) if quantidade_sms > 0 else 0
        # Custo por venda = investimento / total de vendas
        custo_por_venda = investimento / total_vendas if total_vendas > 0 else 0.0
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
                            <span style='color: #fff;'><b>Leads gerados</b></span>
                            <span style='color: #fff;'>{novos_na_carteira}</span>
                        </div>
                        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                            <span style='color: #fff;'><b>Disparos p/ um lead</b></span>
                            <span style='color: #fff;'>{disparos_por_lead:.1f}</span>
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
                            <span style='color: #fff;'><b>Custo por lead</b></span>
                            <span style='color: #fff;'>{formatar_real(custo_por_lead)}</span>
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
        idCampanha = 1  # Fixo
        
        # Debug do período
        print(f"=== DEBUG PERÍODO URA ===")
        print(f"data_ini original: {data_ini}")
        print(f"data_fim original: {data_fim}")
        
        # Ajusta o período para ser mais amplo para teste
        periodoInicial = (datetime.combine(data_ini, datetime.min.time()) - timedelta(days=30)).strftime('%Y-%m-%dT00:00:00')
        periodoFinal = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%dT23:59:59')
        
        print(f"periodoInicial: {periodoInicial}")
        print(f"periodoFinal: {periodoFinal}")
        print(f"=========================")
        
        dados_ura = obter_dados_ura(idCampanha, periodoInicial, periodoFinal)
        
        # Obtém todas as ligações (como é feito com SMS)
        ligacoes_all = dados_ura.get("ligacoes", [])
        
        # Log inicial para debug
        print(f"=== DEBUG URA ===")
        print(f"Período selecionado: {data_ini} a {data_fim}")
        print(f"Tipo de data_ini: {type(data_ini)}")
        print(f"Tipo de data_fim: {type(data_fim)}")
        print(f"Total de ligações retornadas pela API: {len(ligacoes_all)}")
        
        # Debug dos dados retornados
        print(f"Dados URA completos: {dados_ura}")
        
        # Se não há ligações, mostra o que foi retornado pela API
        if not ligacoes_all:
            print("API não retornou ligações. Dados retornados:")
            print(f"Status: {dados_ura.get('codStatus')} - {dados_ura.get('descStatus')}")
            print(f"Qtde Registros: {dados_ura.get('qtdeRegistros')}")
            print(f"Estrutura completa: {dados_ura}")
        
        # Filtra ligações pelo período especificado usando idLigacao e dataHoraLigacao
        ligacoes_periodo = []
        ids_ligacoes = set()  # Para contar IDs únicos
        
        for ligacao in ligacoes_all:
            if isinstance(ligacao, dict):
                # Tenta diferentes campos possíveis para ID e data
                id_ligacao = ligacao.get('idHistoricoTab') or ligacao.get('idLigacao') or ligacao.get('id') or ligacao.get('idRegistro')
                data_hora_ligacao = ligacao.get('dataEvento') or ligacao.get('dataHoraLigacao') or ligacao.get('data') or ligacao.get('dataHora')
                
                print(f"Processando registro ID: {id_ligacao}, Data: {data_hora_ligacao}")
                
                if data_hora_ligacao:
                    try:
                        # Converte a data da ligação para datetime
                        # Remove o timezone se existir e converte
                        data_ligacao_str = data_hora_ligacao.split('.')[0]  # Remove milissegundos
                        if '+' in data_ligacao_str:
                            data_ligacao_str = data_ligacao_str.split('+')[0]  # Remove timezone
                        elif '-' in data_ligacao_str and data_ligacao_str.count('-') > 2:
                            # Remove timezone negativo (-03:00)
                            data_ligacao_str = data_ligacao_str.split('-03:00')[0]
                        
                        data_ligacao = datetime.fromisoformat(data_ligacao_str)
                        
                        # Converte as datas de período para datetime
                        periodo_ini = datetime.combine(data_ini, datetime.min.time())
                        periodo_fim = datetime.combine(data_fim, datetime.max.time())
                        
                        print(f"  Data convertida: {data_ligacao}")
                        print(f"  Período: {periodo_ini} a {periodo_fim}")
                        
                        # Verifica se a ligação está no período
                        if periodo_ini <= data_ligacao <= periodo_fim:
                            ligacoes_periodo.append(ligacao)
                            if id_ligacao:
                                ids_ligacoes.add(id_ligacao)
                            print(f"  ✅ Registro incluído no período")
                        else:
                            print(f"  ❌ Registro fora do período")
                    except Exception as e:
                        # Se não conseguir converter a data, inclui a ligação
                        print(f"  ⚠️ Erro ao processar data: {e} - incluindo registro")
                        ligacoes_periodo.append(ligacao)
                        if id_ligacao:
                            ids_ligacoes.add(id_ligacao)
                else:
                    # Se não tem data, inclui a ligação
                    print(f"  ⚠️ Registro sem data - incluindo")
                    ligacoes_periodo.append(ligacao)
                    if id_ligacao:
                        ids_ligacoes.add(id_ligacao)
        
        # Quantidade de URA = número de IDs únicos de ligações no período
        quantidade_ura = len(ids_ligacoes)
        
        print(f"Ligações únicas no período: {quantidade_ura}")
        print(f"IDs de ligações: {sorted(list(ids_ligacoes))}")
        print(f"==================")
        custo_por_ligacao_ura = dados_ura.get("custo_por_ligacao", 0.034444)
        investimento_ura = quantidade_ura * custo_por_ligacao_ura
        
        # Processa as ligações do período para extrair dados de vendas e atendimentos
        total_vendas_ura = 0
        producao_ura = 0.0
        atendidas_ura = 0
        
        # Analisa as tabulações do período para identificar vendas e atendimentos
        for ligacao in ligacoes_periodo:
            if isinstance(ligacao, dict):
                # Identifica vendas baseado em campos das tabulações
                tabulacao = str(ligacao.get('tabulado', '')).lower()
                categoria_tabulacao = str(ligacao.get('categoriaTabulacao', '')).lower()
                historico = str(ligacao.get('historico', '')).lower()
                resultado = str(ligacao.get('resultadoLigacao', '')).lower()
                status = str(ligacao.get('statusLigacao', '')).lower()
                
                # Debug: mostra algumas registros para entender os dados
                if len(ligacoes_periodo) <= 10:  # Se poucos registros, mostra todas
                    print(f"Processando registro ID {ligacao.get('idHistoricoTab', ligacao.get('idLigacao', ligacao.get('id', 'N/A')))}: tabulado='{tabulacao}', categoria='{categoria_tabulacao}', historico='{historico}', resultado='{resultado}'")
                
                # Identifica vendas baseado em tabulação
                if any(keyword in tabulacao for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                    total_vendas_ura += 1
                    print(f"Venda identificada por tabulação: {tabulacao}")
                
                # Identifica vendas baseado em categoria de tabulação
                if any(keyword in categoria_tabulacao for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                    total_vendas_ura += 1
                    print(f"Venda identificada por categoria: {categoria_tabulacao}")
                
                # Identifica vendas baseado em histórico
                if any(keyword in historico for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido']):
                    total_vendas_ura += 1
                    print(f"Venda identificada por histórico: {historico}")
                
                # Identifica vendas baseado em resultado da ligação
                if any(keyword in resultado for keyword in ['venda', 'vendeu', 'fechou', 'contrato', 'aceitou', 'vendido', 'completada']):
                    total_vendas_ura += 1
                    print(f"Venda identificada por resultado: {resultado}")
                
                # Identifica atendimentos (registros que indicam contato)
                # Registros que não são "NÃO TABULADO" são considerados atendidos
                if tabulacao != 'não tabulado' and tabulacao != 'nao tabulado' and tabulacao:
                    atendidas_ura += 1
                    print(f"Atendimento identificado por tabulação: {tabulacao}")
                
                # Registros com categoria diferente de "NÃO TABULADO" também são atendidos
                if categoria_tabulacao != 'não tabulado' and categoria_tabulacao != 'nao tabulado' and categoria_tabulacao:
                    atendidas_ura += 1
                    print(f"Atendimento identificado por categoria: {categoria_tabulacao}")
                
                # Registros com resultado "COMPLETADA" são considerados atendidos
                if 'completada' in resultado:
                    atendidas_ura += 1
                    print(f"Atendimento identificado por resultado: {resultado}")
        
        # Remove duplicatas de vendas (mesma ligação pode ter múltiplos indicadores)
        total_vendas_ura = min(total_vendas_ura, quantidade_ura)
        
        # Se não encontrou atendimentos específicos, considera todas as registros como atendidos
        if atendidas_ura == 0:
            print("Nenhum atendimento identificado por registro, contando todas as registros...")
            atendidas_ura = len(ligacoes_periodo)
        
        # Calcula métricas derivadas
        previsao_faturamento_ura = producao_ura * 0.171  # Mesmo fator usado no Kolmeya
        ticket_medio_ura = producao_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
        roi_ura = previsao_faturamento_ura - investimento_ura
        
        # Calcula métricas adicionais
        # % que atendem = (atendidas / quantidade_ura) * 100
        percentual_atendem_ura = (atendidas_ura / quantidade_ura * 100) if quantidade_ura > 0 else 0.0
        
        # Leads gerados (assumindo que atendidas são os leads)
        leads_gerados_ura = atendidas_ura
        
        # % de conversão p/lead = (total_vendas / leads_gerados) * 100
        percentual_conversao_lead_ura = (total_vendas_ura / leads_gerados_ura * 100) if leads_gerados_ura > 0 else 0.0
        
        # Lig. atendidas p/ um lead = quantidade_ura / leads_gerados
        ligacoes_por_lead_ura = quantidade_ura / leads_gerados_ura if leads_gerados_ura > 0 else 0.0
        
        # % de leads que converte vendas = (total_vendas / leads_gerados) * 100
        percentual_leads_converte_vendas_ura = (total_vendas_ura / leads_gerados_ura * 100) if leads_gerados_ura > 0 else 0.0
        
        # Ligações p/uma venda = quantidade_ura / total_vendas
        ligacoes_por_venda_ura = quantidade_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
        
        # Custo por lead = investimento / leads_gerados
        custo_por_lead_ura = investimento_ura / leads_gerados_ura if leads_gerados_ura > 0 else 0.0
        
        # Custo por venda = investimento / total_vendas
        custo_por_venda_ura = investimento_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
        
        # Faturamento médio por venda = previsao_faturamento / total_vendas
        faturamento_medio_por_venda_ura = previsao_faturamento_ura / total_vendas_ura if total_vendas_ura > 0 else 0.0
        
        # Log final do processamento
        print(f"\n=== RESUMO URA ===")
        print(f"Período: {data_ini} a {data_fim}")
        print(f"Total de ligações retornadas pela API: {len(ligacoes_all)}")
        print(f"IDs únicos de ligações no período: {quantidade_ura}")
        print(f"IDs das ligações: {sorted(list(ids_ligacoes))}")
        print(f"Ligações atendidas: {atendidas_ura}")
        print(f"Total de vendas: {total_vendas_ura}")
        print(f"Investimento: R$ {investimento_ura:.2f}")
        print(f"==================\n")
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

    # --- BUSCAR TELEFONES DOS JOBS EM AMBOS ---
    telefones_em_ambos = set()  # Garante que sempre existe
    jobs_em_ambos = set()
    if messages_fgts and jobs_em_ambos:
        df_status = pd.DataFrame(messages_fgts)
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
                for m in messages_fgts:
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
            for m in messages_fgts:
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
                for m in messages_fgts:
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

if __name__ == "__main__":
    main()