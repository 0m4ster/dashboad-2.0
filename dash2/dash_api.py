import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional
from facta_token_manager import render_facta_token_page, get_facta_token, is_facta_token_valid
from config import (
    KOLMEYA_API_BASE_URL, 
    KOLMEYA_API_ACCESSES_URL, 
    KOLMEYA_DEFAULT_TOKEN,
    FACTA_API_URLS, 
    FACTA_DEFAULT_TOKEN
)

# Configuração da página
st.set_page_config(
    page_title="SMS Status Dashboard",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# backgroud
st.markdown("""
<style>
    /* Tema claro aprimorado */
    .main {
        background-color: #f0f2f6; /* Fundo mais suave */
        color: #2c3e50; /* Texto mais escuro para contraste */
    }
    
    .stApp {
        background-color: #f0f2f6;
        color: #2c3e50;
    }
    
    .stSidebar {
        background-color: #ffffff; /* Sidebar branco */
        color: #2c3e50;
        border-right: 1px solid #e0e0e0; /* Borda sutil */
    }
    
    .stSidebar .sidebar-content {
        background-color: #ffffff;
        color: #2c3e50;
    }
    
    /* Texto geral */
    h1, h2, h3, h4, h5, h6, p, div, span {
        color: #2c3e50 !important;
    }
    
    /* Métricas */
    .metric-container {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 10px; /* Bordas mais arredondadas */
        padding: 15px; /* Mais padding */
        box-shadow: 0 4px 12px rgba(0,0,0,0.08); /* Sombra mais pronunciada */
        transition: all 0.3s ease-in-out; /* Transição suave */
        margin-bottom: 10px;
    }
    .metric-container:hover {
        transform: translateY(-3px); /* Efeito hover */
        box-shadow: 0 6px 16px rgba(0,0,0,0.12);
    }
    
    /* Dataframes */
    .dataframe {
        background-color: #ffffff !important;
        color: #2c3e50 !important;
        border-radius: 8px;
        overflow: hidden; /* Para bordas arredondadas */
    }
    
    .dataframe th {
        background-color: #e9ecef !important; /* Cabeçalho mais claro */
        color: #2c3e50 !important;
        border: none !important; /* Remover bordas internas */
        padding: 12px 8px !important;
    }
    
    .dataframe td {
        background-color: #ffffff !important;
        color: #2c3e50 !important;
        border-top: 1px solid #f0f0f0 !important; /* Linhas divisórias mais claras */
        padding: 10px 8px !important;
    }
    
    /* Inputs e selects */
    .stTextInput, .stSelectbox, .stMultiselect, .stDateInput {
        background-color: #ffffff !important;
        color: #2c3e50 !important;
        border-radius: 6px;
    }
    
    .stTextInput input, .stSelectbox select, .stMultiselect select {
        background-color: #ffffff !important;
        color: #2c3e50 !important;
        border: 1px solid #d0d0d0 !important; /* Borda mais suave */
        border-radius: 6px !important;
        padding: 8px 12px;
    }
    
    /* Botões */
    .stButton > button {
        background-color: #3498db !important; /* Azul mais vibrante */
        color: #ffffff !important;
        border: none !important;
        border-radius: 6px !important;
        padding: 10px 20px !important;
        font-weight: bold;
        transition: background-color 0.2s ease-in-out;
        width: 100%;
        margin-bottom: 5px;
    }
    
    .stButton > button:hover {
        background-color: #2980b9 !important; /* Tom mais escuro no hover */
        color: #ffffff !important;
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background-color: #f8f9fa !important;
        color: #2c3e50 !important;
        border: 1px solid #e0e0e0 !important;
        border-radius: 8px;
        margin-bottom: 10px;
    }
    
    /* Progress bars */
    .stProgress > div > div > div {
        background-color: #e0e0e0 !important;
        border-radius: 5px;
    }
    
    .stProgress > div > div > div > div {
        background-color: #3498db !important;
        border-radius: 5px;
    }
    
    /* Sidebar inputs */
    .css-1d391kg {
        background-color: #ffffff !important;
    }
    
    /* File uploader */
    .stFileUploader {
        background-color: #ffffff !important;
        border: 1px solid #d0d0d0 !important;
        border-radius: 6px;
    }
    
    /* Success, warning, error messages */
    .stAlert {
        border-radius: 8px;
        padding: 15px;
        font-weight: 500;
    }
    
    .stInfo {
        background-color: #e8f4f8 !important; /* Azul claro suave */
        border: 1px solid #b3e0f2 !important;
        color: #2196f3 !important;
    }
    
    .stWarning {
        background-color: #fff3e0 !important; /* Laranja claro suave */
        border: 1px solid #ffe0b2 !important;
        color: #ff9800 !important;
    }
    
    .stError {
        background-color: #ffebee !important; /* Vermelho claro suave */
        border: 1px solid #ffcdd2 !important;
        color: #f44336 !important;
    }
    
    .stSuccess {
        background-color: #e8f5e9 !important; /* Verde claro suave */
        border: 1px solid #c8e6c9 !important;
        color: #4caf50 !important;
    }
    
    /* Melhorias para alinhamento */
    .stMetric {
        text-align: center !important;
    }
    
    .stMetric > div {
        text-align: center !important;
    }
    
    /* Container para métricas */
    .metric-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
    }
    
    /* Espaçamento consistente */
    .section-spacing {
        margin-top: 30px;
        margin-bottom: 30px;
    }
    
    /* Cards para seções */
    .section-card {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* Responsividade */
    @media (max-width: 768px) {
        .metric-container {
            margin-bottom: 15px;
        }
        
        .stButton > button {
            padding: 8px 16px !important;
            font-size: 14px !important;
        }
        
        h1, h2, h3, h4, h5, h6 {
            font-size: 1.1em !important;
        }
    }
    
    /* Melhorias para espaçamento */
    .stMarkdown {
        margin-bottom: 1rem;
    }
    
    /* Container principal */
    .main-container {
        max-width: 100%;
        margin: 0 auto;
        padding: 0 20px;
    }
    
    /* Melhorias para métricas */
    .metric-value {
        font-size: 24px !important;
        font-weight: bold !important;
        color: #2c3e50 !important;
    }
    
    .metric-label {
        font-size: 14px !important;
        color: #7f8c8d !important;
        margin-bottom: 5px !important;
    }
</style>
""", unsafe_allow_html=True)


# Configurações da API (agora importadas do config.py)
API_BASE_URL = KOLMEYA_API_BASE_URL
API_ACCESSES_URL = KOLMEYA_API_ACCESSES_URL
DEFAULT_TOKEN = KOLMEYA_DEFAULT_TOKEN

def testar_conexao_api(token: str) -> bool:
    """
    Testa a conexão com a API usando dados de exemplo.
    Retorna True se a conexão for bem-sucedida, False caso contrário.
    """
    try:
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2)
        
        payload = {
            "start_at": start_date.strftime('%Y-%m-%d %H:%M'),
            "end_at": end_date.strftime('%Y-%m-%d %H:%M'),
            "limit": 1
        }
        
        response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            return True
        else:
            # Não exibir erro detalhado, apenas retornar False
            return False
            
    except requests.exceptions.RequestException as e:
        # Não exibir erro detalhado, apenas retornar False
        return False
    except Exception as e:
        # Não exibir erro detalhado, apenas retornar False
        return False



@st.cache_data(ttl=300)  # Cache por 5 minutos
def obter_relatorio_sms_paginado(start_at: str, end_at: str, token: str, centro_custo: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Obtém todos os envios de SMS da API Kolmeya com múltiplas consultas automáticas,
    dividindo o período em intervalos menores para garantir a recuperação de todos os dados.
    
    Args:
        start_at: Data inicial (Y-m-d H:i)
        end_at: Data final (Y-m-d H:i)
        token: Token de autorização
        centro_custo: Centro de custo para filtrar os envios.
    
    Returns:
        DataFrame com todos os envios no período ou None se houver erro.
    """
    try:
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        if not start_at or not end_at:
            st.error("Datas de início e fim são obrigatórias.")
            return None
        
        start_date_dt = datetime.strptime(start_at, '%Y-%m-%d %H:%M')
        end_date_dt = datetime.strptime(end_at, '%Y-%m-%d %H:%M')
        
        if (end_date_dt - start_date_dt).days > 7:
            st.error("O período máximo permitido é de 7 dias.")
            return None
        
        todos_dados = []
        total_registros_obtidos = 0
        
        # A lista de intervalos será preenchida dinamicamente
        intervalos_para_buscar = [(start_date_dt, end_date_dt)]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Limite de registros 
        API_LIMIT = 30000
        
        # Loop principal para buscar dados
        i = 0
        while intervalos_para_buscar:
            current_interval_start, current_interval_end = intervalos_para_buscar.pop(0)
            
            # Atualizar status da barra de progresso
            i += 1
            progress = min(1.0, i / 1000)  
            status_text.text(f"Buscando dados... Intervalo {i} ({current_interval_start.strftime('%H:%M')} - {current_interval_end.strftime('%H:%M')})")
            progress_bar.progress(progress)
            
            payload = {
                "start_at": current_interval_start.strftime('%Y-%m-%d %H:%M'),
                "end_at": current_interval_end.strftime('%Y-%m-%d %H:%M'),
                "limit": API_LIMIT
            }
            if centro_custo:
                payload["centro_custo"] = centro_custo
            
            try:
                response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=30)
                response.raise_for_status() 
                
                data = response.json()
                
                if 'messages' in data and data['messages']:
                    dados_obtidos_no_intervalo = data['messages']
                    todos_dados.extend(dados_obtidos_no_intervalo)
                    total_registros_obtidos += len(dados_obtidos_no_intervalo)
                
                    if len(dados_obtidos_no_intervalo) >= API_LIMIT:
                        
                        mid_point = current_interval_start + (current_interval_end - current_interval_start) / 2
                        if mid_point > current_interval_start: # Evitar loops 
                            intervalos_para_buscar.insert(0, (current_interval_start, mid_point))
                            intervalos_para_buscar.insert(1, (mid_point, current_interval_end))
                        else:
                            #se intervalo curto
                            temp_start = current_interval_start
                            while temp_start < current_interval_end:
                                temp_end = min(temp_start + timedelta(minutes=1), current_interval_end)
                                if temp_end > temp_start:
                                    intervalos_para_buscar.insert(0, (temp_start, temp_end))
                                temp_start = temp_end
                            
            except requests.exceptions.HTTPError as http_err:
                # Tratar erro 422 silenciosamente (dados inválidos para o intervalo)
                if http_err.response.status_code != 422:
                    # Apenas logar outros erros HTTP, mas continuar
                    continue
                # Para erro 422, apenas continuar sem mostrar mensagem
                continue
            except (requests.exceptions.RequestException, json.JSONDecodeError, Exception):
                # Tratar todos os outros erros silenciosamente
                continue
        
        progress_bar.empty()
        status_text.empty()
        
        if not todos_dados:
            # Token inválido ou sem dados - retornar silenciosamente
            return None
        
        # Limpar e converter dados para DataFrame
        dados_limpos = []
        for registro in todos_dados:
            registro_limpo = {k: str(v) if isinstance(v, (dict, list)) else v for k, v in registro.items()}
            dados_limpos.append(registro_limpo)
        
        df = pd.DataFrame(dados_limpos)
        
        # Filtrar por centro de custo 
        if centro_custo and 'centro_custo' in df.columns:
            df_antes_filtro = len(df)
            df['centro_custo'] = df['centro_custo'].astype(str) # Garantir tipo string
            
            # Tentar match exato primeiro
            df_filtrado = df[df['centro_custo'] == centro_custo]
            
        # Converter tipos de dados
        df['enviada_em'] = pd.to_datetime(df['enviada_em'], format='%d/%m/%Y %H:%M', errors='coerce')
        df['lote'] = pd.to_numeric(df['lote'], errors='coerce')
        df['job'] = pd.to_numeric(df['job'], errors='coerce')
        
        # Filtrar dados que estão fora do período solicitado (pode ocorrer devido à sobreposição de intervalos)
        df = df[(df['enviada_em'] >= start_date_dt) & (df['enviada_em'] <= end_date_dt)]
        
        # Remover duplicatas
        df.drop_duplicates(inplace=True)
        
        return df
        
    except Exception as e:
        st.error(f"Erro geral ao obter relatório SMS: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache por 5 minutos
def obter_acessos_encurtador(start_at: str, end_at: str, token: str, tenant_segment_id: Optional[int] = None, is_robot: Optional[int] = None, limit: int = 5000, acessos_unicos: bool = True) -> tuple[Optional[pd.DataFrame], int]:
    """
    Obtém os acessos realizados nas mensagens SMS enviadas (endpoint de encurtador).
    
    Args:
        start_at: Data inicial (Y-m-d)
        end_at: Data final (Y-m-d)
        token: Token de autorização
        tenant_segment_id: ID do centro de custo específico (opcional)
        is_robot: Filtro para robôs (0 = não robô, 1 = robô)
        limit: Limite de registros (máximo 5000)
    
    Returns:
        DataFrame com os acessos no período ou None se houver erro.
    """
    try:
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        if not start_at or not end_at:
            st.error("Datas de início e fim são obrigatórias.")
            return None, 0
        
        # Converter datas para formato Y-m-d se necessário
        if ' ' in start_at:
            start_at = start_at.split(' ')[0]
        if ' ' in end_at:
            end_at = end_at.split(' ')[0]
        
        # IMPLEMENTAR PAGINAÇÃO AUTOMÁTICA para garantir todos os registros
        all_accesses = []
        total_accesses = 0
        page = 1
        max_pages = 50  
        
        while page <= max_pages:
            payload = {
                "start_at": start_at,
                "end_at": end_at,
                "limit": min(limit, 5000),  # Garantir que não exceda o limite da API
                "page": page
            }
            
            if tenant_segment_id is not None:
                payload["tenant_segment_id"] = tenant_segment_id
            
            if is_robot is not None:
                payload["is_robot"] = is_robot
            
            response = requests.post(API_ACCESSES_URL, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Processar resposta da API
            current_accesses = []
            current_total = 0
            
            if isinstance(data, list):
                # Verificar se é uma lista de objetos com 'accesses' e 'totalAccesses'
                if data and isinstance(data[0], dict) and 'accesses' in data[0]:
                    for item in data:
                        if isinstance(item, dict) and 'accesses' in item:
                            if isinstance(item['accesses'], list):
                                current_accesses.extend(item['accesses'])
                            current_total += item.get('totalAccesses', 0)
                else:
                    # Formato: lista direta de acessos
                    current_accesses = data
                    current_total = len(data)
            elif isinstance(data, dict) and 'accesses' in data:
                # Formato: dicionário com 'accesses' e 'totalAccesses'
                current_accesses = data['accesses']
                current_total = data.get('totalAccesses', len(current_accesses))
            else:
                st.error(f"Formato de resposta da API não reconhecido na página {page}")
                break
            
            # Adicionar acessos da página atual
            if current_accesses:
                all_accesses.extend(current_accesses)
                total_accesses = max(total_accesses, current_total)
                
                # Se recebemos menos registros que o limite, provavelmente é a última página
                if len(current_accesses) < limit:
                    break
            else:
                # Página vazia, parar
                break
            
            page += 1
            
            # Pequena pausa para não sobrecarregar a API
            import time
            time.sleep(0.1)
        
        if not all_accesses:
            st.warning("Nenhum acesso encontrado para o período selecionado.")
            return None, total_accesses
        
        # Verificar se accesses é uma lista válida
        if not isinstance(all_accesses, list):
            st.error(f"Lista de acessos não é válida: {type(all_accesses)}")
            return None, total_accesses
        
        # Converter para DataFrame
        df = pd.DataFrame(all_accesses)
        
        # Converter tipos de dados
        if 'accessed_at' in df.columns:
            df['accessed_at'] = pd.to_datetime(df['accessed_at'], errors='coerce')
        if 'job_created_at' in df.columns:
            df['job_created_at'] = pd.to_datetime(df['job_created_at'], errors='coerce')
        if 'job_id' in df.columns:
            df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce')
        if 'tenant_segment_id' in df.columns:
            df['tenant_segment_id'] = pd.to_numeric(df['tenant_segment_id'], errors='coerce')
        if 'fullphone' in df.columns:
            df['fullphone'] = pd.to_numeric(df['fullphone'], errors='coerce')
        if 'cpf' in df.columns:
            df['cpf'] = pd.to_numeric(df['cpf'], errors='coerce')
        if 'is_robot' in df.columns:
            df['is_robot'] = df['is_robot'].astype(str)
        
        # Remover duplicatas para obter acessos únicos (se solicitado)
        if acessos_unicos:
            df_antes = len(df)
            
            # Definir campos para identificar duplicatas (priorizando os mais importantes)
            campos_unicos = []
            if 'fullphone' in df.columns and 'cpf' in df.columns:
                campos_unicos = ['fullphone', 'cpf', 'tenant_segment_id']
            elif 'fullphone' in df.columns:
                campos_unicos = ['fullphone', 'tenant_segment_id']
            elif 'cpf' in df.columns:
                campos_unicos = ['cpf', 'tenant_segment_id']
            elif 'job_id' in df.columns:
                campos_unicos = ['job_id', 'fullphone'] if 'fullphone' in df.columns else ['job_id']
            else:
                # Se não tiver campos específicos, usar todos os campos disponíveis
                campos_unicos = df.columns.tolist()
            
            # Remover duplicatas baseadas nos campos únicos
            df = df.drop_duplicates(subset=campos_unicos, keep='first')
            
            df_depois = len(df)
            duplicatas_removidas = df_antes - df_depois
            
        return df, total_accesses
        
    except requests.exceptions.HTTPError as http_err:
        # Não exibir erros detalhados, apenas retornar None
        return None, 0
    except requests.exceptions.RequestException as req_err:
        # Não exibir erros detalhados, apenas retornar None
        return None, 0
    except json.JSONDecodeError:
        # Não exibir erros detalhados, apenas retornar None
        return None, 0
    except Exception as e:
        # Não exibir erros detalhados, apenas retornar None
        return None, 0

# @st.cache_data(ttl=300)  # Cache por 5 minutos
def obter_propostas_facta(token: str, ambiente: str = 'producao', cpfs_validos: Optional[list] = None, **kwargs) -> Optional[pd.DataFrame]:
    """
    Obtém o andamento de propostas da API FACTA.
    
    Args:
        token: Token de autorização Bearer
        ambiente: 'homologacao' ou 'producao'
        cpfs_validos: Lista de CPFs válidos para filtrar resultados (apenas clientes do endpoint de acessos)
        **kwargs: Parâmetros opcionais da API:
            - convenio: int (ex: 3)
            - averbador: int (código do averbador)
            - af: int (código AF)
            - cpf: str (CPF específico para consulta)
            - data_ini: str (DD/MM/AAAA)
            - data_fim: str (DD/MM/AAAA)
            - data_alteracao_ini: str (DD/MM/AAAA)
            - data_alteracao_fim: str (DD/MM/AAAA)
            - pagina: int
            - quantidade: int (1-5000)
            - consulta_sub: str ('S' ou 'N')
            - codigo_sub: int
    
    Returns:
        DataFrame com as propostas filtradas apenas para CPFs válidos ou None se houver erro.
    """
    try:

        
        if ambiente not in FACTA_API_URLS:
            st.error("Ambiente inválido. Use 'homologacao' ou 'producao'.")
            return None
        
        url = FACTA_API_URLS[ambiente]
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        # Construir parâmetros da query
        params = {}
        
        # Parâmetros opcionais
        if 'convenio' in kwargs and kwargs['convenio']:
            params['convenio'] = kwargs['convenio']
        if 'averbador' in kwargs and kwargs['averbador']:
            params['averbador'] = kwargs['averbador']
        if 'af' in kwargs and kwargs['af']:
            params['af'] = kwargs['af']
        if 'cpf' in kwargs and kwargs['cpf']:
            params['cpf'] = kwargs['cpf']
        if 'data_ini' in kwargs and kwargs['data_ini']:
            params['data_ini'] = kwargs['data_ini']
        if 'data_fim' in kwargs and kwargs['data_fim']:
            params['data_fim'] = kwargs['data_fim']
        if 'data_alteracao_ini' in kwargs and kwargs['data_alteracao_ini']:
            params['data_alteracao_ini'] = kwargs['data_alteracao_ini']
        if 'data_alteracao_fim' in kwargs and kwargs['data_alteracao_fim']:
            params['data_alteracao_fim'] = kwargs['data_alteracao_fim']
        if 'pagina' in kwargs and kwargs['pagina']:
            params['pagina'] = kwargs['pagina']
        if 'quantidade' in kwargs and kwargs['quantidade']:
            params['quantidade'] = min(kwargs['quantidade'], 5000)  # Limite máximo
        if 'consulta_sub' in kwargs and kwargs['consulta_sub']:
            params['consulta_sub'] = kwargs['consulta_sub']
        if 'codigo_sub' in kwargs and kwargs['codigo_sub']:
            params['codigo_sub'] = kwargs['codigo_sub']
        
        # Verificar limite de período para consulta_sub
        if 'consulta_sub' in params and params['consulta_sub'] == 'S':
            if 'data_ini' in params and 'data_fim' in params:
                try:
                    data_ini = datetime.strptime(params['data_ini'], '%d/%m/%Y')
                    data_fim = datetime.strptime(params['data_fim'], '%d/%m/%Y')
                    if (data_fim - data_ini).days > 10:
                        st.warning("⚠️ Para consulta_sub='S', o período máximo é de 10 dias. Ajustando...")
                        params['data_fim'] = (data_ini + timedelta(days=10)).strftime('%d/%m/%Y')
                except:
                    pass
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('erro', False):
            st.error(f"Erro na API FACTA: {data.get('mensagem', 'Erro desconhecido')}")
            return None
        
        propostas = data.get('propostas', [])
        if not propostas:
            st.warning("Nenhuma proposta encontrada para os parâmetros informados.")
            return None
        
        # Converter para DataFrame
        df = pd.DataFrame(propostas)
        
        # Converter tipos de dados
        if 'data_movimento' in df.columns:
            df['data_movimento'] = pd.to_datetime(df['data_movimento'], format='%d/%m/%Y', errors='coerce')
        if 'data_digitacao' in df.columns:
            df['data_digitacao'] = pd.to_datetime(df['data_digitacao'], format='%d/%m/%Y', errors='coerce')
        if 'data_efetivacao' in df.columns:
            df['data_efetivacao'] = pd.to_datetime(df['data_efetivacao'], format='%d/%m/%Y', errors='coerce')
        if 'data_pgto_cliente' in df.columns:
            df['data_pgto_cliente'] = pd.to_datetime(df['data_pgto_cliente'], format='%d/%m/%Y', errors='coerce')
        
        # Converter valores numéricos
        numeric_columns = ['vlrprestacao', 'numeroprestacao', 'saldo_devedor', 'valor_af', 
                          'valor_bruto', 'taxa', 'valor_iof', 'valor_seguro', 'vendedor']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Converter CPF para string formatada
        if 'cpf' in df.columns:
            df['cpf'] = df['cpf'].astype(str).str.zfill(11)
        
        # FILTRAGEM OBRIGATÓRIA: Aplicar filtro de CPFs válidos se fornecido
        if cpfs_validos and 'cpf' in df.columns:
            # Normalizar CPFs do Kolmeya para comparação
            cpfs_kolmeya_norm = [str(cpf).zfill(11) for cpf in cpfs_validos]
            
            # Normalizar CPFs da FACTA
            df['cpf_norm'] = df['cpf'].astype(str).str.zfill(11)
            
            # APLICAR FILTRO: Manter apenas propostas com CPFs que existem nos acessos do Kolmeya
            df_filtrado = df[df['cpf_norm'].isin(cpfs_kolmeya_norm)]
            
            # Remover coluna temporária
            df_filtrado = df_filtrado.drop('cpf_norm', axis=1)
            
            # Calcular estatísticas do filtro
            total_antes = len(df)
            total_depois = len(df_filtrado)
            cpfs_unicos_depois = df_filtrado['cpf'].nunique()
            
            # Filtro aplicado com sucesso (sem interface adicional)
            
            # Validação do filtro
            if total_depois == 0:
                st.warning(f"⚠️ Nenhuma proposta encontrada para os {len(cpfs_validos):,} CPFs dos acessos")
                return None
            
            # Validação silenciosa dos CPFs retornados
            cpfs_retornados = set(df_filtrado['cpf'].unique())
            cpfs_validos_set = set(cpfs_kolmeya_norm)
            
            cpfs_invalidos = cpfs_retornados - cpfs_validos_set
            if cpfs_invalidos:
                st.error(f"❌ ERRO: {len(cpfs_invalidos)} CPFs retornados não estão na lista de CPFs válidos")
                return None
            
            return df_filtrado
            
        elif cpfs_validos:
            st.error(f"❌ ERRO: CPFs válidos fornecidos ({len(cpfs_validos)}), mas coluna 'cpf' não encontrada no DataFrame")
            return None
        else:
            st.error(f"❌ ERRO: Nenhum filtro de CPFs aplicado")
            return None
        
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 401:
            st.error("Erro 401: Token inválido ou expirado.")
        elif http_err.response.status_code == 403:
            st.error("Erro 403: Acesso negado.")
        else:
            st.error(f"Erro HTTP ao buscar propostas: {http_err.response.status_code} - {http_err.response.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        st.error(f"Erro de requisição ao buscar propostas: {req_err}")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro ao decodificar JSON da resposta. Resposta: {response.text}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao buscar propostas: {str(e)}")
        return None

def diagnosticar_dados(df: pd.DataFrame) -> Dict:
    """Diagnostica problemas nos dados e retorna informações úteis."""
    if df is None or df.empty:
        return {}
        
    diagnostico = {
        'total_registros': len(df),
        'total_colunas': len(df.columns),
        'colunas_problematicas': [],
        'tipos_dados': {col: str(df[col].dtype) for col in df.columns},
        'valores_nulos': {col: df[col].isnull().sum() for col in df.columns},
        'exemplos_problematicos': {}
    }
    
    for col in df.columns:
        try:
            # Tentar operações básicas para identificar problemas
            df[col].unique()
            df[col].value_counts()
        except Exception as e:
            diagnostico['colunas_problematicas'].append({
                'coluna': col,
                'erro': str(e),
                'tipo': str(df[col].dtype)
            })
            try:
                diagnostico['exemplos_problematicos'][col] = df[col].head(5).tolist()
            except:
                diagnostico['exemplos_problematicos'][col] = ['Não foi possível obter exemplos.']
    
    return diagnostico

def normalizar_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a coluna 'status_norm' padronizada a partir de qualquer coluna de status disponível.
    Prioriza 'status', mas busca por outras colunas que contenham 'status' no nome.
    """
    if df is None or df.empty:
        return df

    status_column_name = None
    if 'status' in df.columns:
        status_column_name = 'status'
    else:
        for col in df.columns:
            if 'status' in col.lower() and df[col].notna().any():
                status_column_name = col
                break

    if status_column_name is None:
        df['status_norm'] = 'DESCONHECIDO'
        return df

    status_series = df[status_column_name].astype(str).str.upper().str.strip()

    # Mapeamento de status para padronização
    status_map = {
        'ENTREGUE': 'DELIVERED', 'ENTREGUES': 'DELIVERED',
        'ENVIADO': 'SENT', 'ENVIADA': 'SENT', 'ENVIADOS': 'SENT',
        'FALHA': 'FAILED', 'ERRO': 'FAILED',
        'REJEITADO': 'REJECTED', 'REJEITADA': 'REJECTED', 'REJEITADOS': 'REJECTED',
        'NAO_ENTREGUE': 'UNDELIVERED', 'NÃO_ENTREGUE': 'UNDELIVERED',
        'NAO ENTREGUE': 'UNDELIVERED', 'NÃO ENTREGUE': 'UNDELIVERED',
        'PENDENTE': 'PENDING', 'AGUARDANDO': 'PENDING',
        'EM PROCESSAMENTO': 'PROCESSING',
        'AGENDADO': 'SCHEDULED',
        'EXPIRADO': 'EXPIRED',
        'BLOQUEADO': 'BLOCKED',
        # Adicionar outros status comuns que possam aparecer e queira padronizar
        'DELIVERED_TO_OPERATOR': 'DELIVERED',
        'DELIVERED_TO_HANDSET': 'DELIVERED',
        'SUBMITTED': 'SENT',
        'ACCEPTED': 'SENT',
        'OK': 'SENT',
        'SUCCESS': 'SENT',
        'NOT_DELIVERED': 'UNDELIVERED',
        'CANCELED': 'FAILED', 'CANCELLED': 'FAILED'
    }
    
    # Aplicar o mapeamento
    df['status_norm'] = status_series.replace(status_map).fillna('DESCONHECIDO')
    
    return df

def calcular_metricas(df: pd.DataFrame) -> Dict:
    """Calcula métricas importantes dos dados."""
    if df is None or df.empty:
        return {}
    
    df = normalizar_status(df.copy())
    
    total_mensagens = len(df)
    status_counts = df['status_norm'].value_counts()
    
    # Definir conjuntos de status para categorização
    success_statuses = {'DELIVERED', 'SENT'}
    failure_statuses = {'REJECTED', 'UNDELIVERED', 'FAILED', 'BLOCKED', 'EXPIRED'}
    pending_statuses = {'PENDING', 'PROCESSING', 'SCHEDULED'}
    
    sucesso = df['status_norm'].isin(success_statuses).sum()
    falhas = df['status_norm'].isin(failure_statuses).sum()
    pendentes = df['status_norm'].isin(pending_statuses).sum()
    
    taxa_sucesso = (sucesso / total_mensagens * 100) if total_mensagens > 0 else 0
    taxa_falha = (falhas / total_mensagens * 100) if total_mensagens > 0 else 0
    taxa_pendente = (pendentes / total_mensagens * 100) if total_mensagens > 0 else 0
    
    return {
        'total_mensagens': total_mensagens,
        'taxa_sucesso': taxa_sucesso,
        'taxa_falha': taxa_falha,
        'taxa_pendente': taxa_pendente,
        'status_counts': status_counts,
        'sucesso': sucesso,
        'falhas': falhas,
        'pendentes': pendentes
    }

def criar_grafico_status(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de pizza com distribuição de status."""
    if df is None or df.empty:
        return go.Figure().update_layout(title="Distribuição de Status das Mensagens (Sem Dados)")
    
    df = normalizar_status(df.copy())
    status_counts = df['status_norm'].value_counts()
    
    # Mapeamento de status para português
    status_portugues = {
        'DELIVERED': 'ENTREGUES',
        'SENT': 'ENVIADAS',
        'FAILED': 'FALHA',
        'UNDELIVERED': 'NÃO ENTREGUES',
        'REJECTED': 'REJEITADAS',
        'PENDING': 'PENDENTES',
        'PROCESSING': 'EM PROCESSAMENTO',
        'SCHEDULED': 'AGENDADAS',
        'BLOCKED': 'BLOQUEADAS',
        'EXPIRED': 'EXPIRADAS',
        'DESCONHECIDO': 'DESCONHECIDO'
    }
    
    # Definir cores para os status principais
    color_map = {
        'DELIVERED': '#28a745',  # Verde para sucesso
        'SENT': '#17a2b8',       # Azul claro para enviado 
        'FAILED': '#dc3545',     # Vermelho para falha
        'UNDELIVERED': '#ffc107',# Amarelo para não entregue
        'REJECTED': '#6c757d',   # Cinza para rejeitado
        'PENDING': '#007bff',    # Azul para pendente
        'PROCESSING': '#fd7e14', # Laranja para processando
        'SCHEDULED': '#6f42c1',  # Roxo para agendado
        'BLOCKED': '#343a40',    # Cinza escuro para bloqueado
        'EXPIRED': '#e83e8c',    # Rosa para expirado
        'DESCONHECIDO': '#adb5bd' # Cinza claro para desconhecido
    }
    
    # Mapear cores para os status presentes nos dados
    pie_colors = [color_map.get(s, '#adb5bd') for s in status_counts.index] # Default para cinza claro
    
    # Calcular percentuais para exibir na legenda
    total = status_counts.sum()
    percentuais = [(count / total * 100) for count in status_counts.values]
    
    # Criar labels com percentuais em português
    labels_with_percent = [f"{status_portugues.get(status, status)} ({percent:.1f}%)" for status, percent in zip(status_counts.index, percentuais)]
    
    fig = go.Figure(data=[go.Pie(
        labels=labels_with_percent,
        values=status_counts.values,
        hole=0.3,
        marker_colors=pie_colors,
        name="Status",
        textinfo='label+percent',
        textposition='outside',
        textfont=dict(size=12, color='#333333')
    )])
    
    fig.update_layout(
        title={
            'text': "Distribuição de Status das Mensagens",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#2c3e50'}
        },
        showlegend=True,
        height=450,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1.02,
            xanchor="left",
            x=1.05,
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='#e0e0e0',
            borderwidth=1,
            font=dict(size=12, color='#2c3e50'),
            itemsizing='constant'
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(color='#333333'),
        margin=dict(r=200)  # Espaço extra para a legenda
    )
    
    return fig

def criar_grafico_timeline(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de linha temporal da quantidade de mensagens por status."""
    if df is None or df.empty:
        return go.Figure().update_layout(title="Evolução dos Status ao Longo do Tempo (Sem Dados)")
    
    df = normalizar_status(df.copy())
    
    # Mapeamento de status para português
    status_portugues = {
        'DELIVERED': 'ENTREGUES',
        'SENT': 'ENVIADAS',
        'FAILED': 'FALHA',
        'UNDELIVERED': 'NÃO ENTREGUES',
        'REJECTED': 'REJEITADAS',
        'PENDING': 'PENDENTES',
        'PROCESSING': 'EM PROCESSAMENTO',
        'SCHEDULED': 'AGENDADAS',
        'BLOCKED': 'BLOQUEADAS',
        'EXPIRED': 'EXPIRADAS',
        'DESCONHECIDO': 'DESCONHECIDO'
    }
    
    # Agrupar por data (dia) e status
    df_timeline = df.groupby([df['enviada_em'].dt.date, 'status_norm']).size().reset_index(name='count')
    df_timeline['enviada_em'] = pd.to_datetime(df_timeline['enviada_em'])
    
    fig = go.Figure()
    
    # Definir cores para os status principais (consistente com o gráfico de pizza)
    color_map = {
        'DELIVERED': '#28a745',
        'SENT': '#17a2b8',
        'FAILED': '#dc3545',
        'UNDELIVERED': '#ffc107',
        'REJECTED': '#6c757d',
        'PENDING': '#007bff',
        'PROCESSING': '#fd7e14',
        'SCHEDULED': '#6f42c1',
        'BLOCKED': '#343a40',
        'EXPIRED': '#e83e8c',
        'DESCONHECIDO': '#adb5bd'
    }
    
    # Ordenar status para consistência na visualização (opcional)
    sorted_statuses = sorted(df_timeline['status_norm'].unique(), key=lambda x: (x not in color_map, x))
    
    for status in sorted_statuses:
        data_status = df_timeline[df_timeline['status_norm'] == status]
        total_status = data_status['count'].sum()
        status_portugues_nome = status_portugues.get(status, status)
        fig.add_trace(go.Scatter(
            x=data_status['enviada_em'],
            y=data_status['count'],
            mode='lines+markers',
            name=f"{status_portugues_nome} ({total_status:,})",
            line=dict(color=color_map.get(status, '#adb5bd'), width=2),
            marker=dict(size=6, color=color_map.get(status, '#adb5bd')),
            stackgroup='one', # Para criar um gráfico de área empilhada
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Data: %{x}<br>' +
                         'Quantidade: %{y:,}<br>' +
                         '<extra></extra>'
        ))
    
    fig.update_layout(
        title={
            'text': "Evolução dos Status ao Longo do Tempo",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#2c3e50'}
        },
        xaxis_title="Data",
        yaxis_title="Quantidade de Mensagens",
        height=450,
        hovermode="x unified", 
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(color='#333333'),
        xaxis=dict(
            gridcolor='#e9ecef',
            title_font=dict(size=14, color='#2c3e50'),
            tickfont=dict(size=12, color='#2c3e50')
        ),
        yaxis=dict(
            gridcolor='#e9ecef',
            title_font=dict(size=14, color='#2c3e50'),
            tickfont=dict(size=12, color='#2c3e50')
        ),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1.02,
            xanchor="left",
            x=1.05,
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='#e0e0e0',
            borderwidth=1,
            font=dict(size=11, color='#2c3e50'),
            itemsizing='constant'
        ),
        margin=dict(r=200)  # Espaço extra para a legenda
    )
    
    return fig

def criar_grafico_barras_status(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de barras com distribuição de status."""
    if df is None or df.empty:
        return go.Figure().update_layout(title="Distribuição de Status das Mensagens (Sem Dados)")
    
    df = normalizar_status(df.copy())
    status_counts = df['status_norm'].value_counts()
    
    # Mapeamento de status para português
    status_portugues = {
        'DELIVERED': 'ENTREGUES',
        'SENT': 'ENVIADAS',
        'FAILED': 'FALHA',
        'UNDELIVERED': 'NÃO ENTREGUES',
        'REJECTED': 'REJEITADAS',
        'PENDING': 'PENDENTES',
        'PROCESSING': 'EM PROCESSAMENTO',
        'SCHEDULED': 'AGENDADAS',
        'BLOCKED': 'BLOQUEADAS',
        'EXPIRED': 'EXPIRADAS',
        'DESCONHECIDO': 'DESCONHECIDO'
    }
    
    # Definir cores para os status principais
    color_map = {
        'DELIVERED': '#28a745',  # Verde para sucesso
        'SENT': '#17a2b8',       # Azul claro para enviado 
        'FAILED': '#dc3545',     # Vermelho para falha
        'UNDELIVERED': '#ffc107',# Amarelo para não entregue
        'REJECTED': '#6c757d',   # Cinza para rejeitado
        'PENDING': '#007bff',    # Azul para pendente
        'PROCESSING': '#fd7e14', # Laranja para processando
        'SCHEDULED': '#6f42c1',  # Roxo para agendado
        'BLOCKED': '#343a40',    # Cinza escuro para bloqueado
        'EXPIRED': '#e83e8c',    # Rosa para expirado
        'DESCONHECIDO': '#adb5bd' # Cinza claro para desconhecido
    }
    
    # Mapear cores para os status presentes nos dados
    bar_colors = [color_map.get(s, '#adb5bd') for s in status_counts.index]
    
    # Calcular percentuais
    total = status_counts.sum()
    percentuais = [(count / total * 100) for count in status_counts.values]
    
    # Criar texto para as barras com status em português
    text_values = [f"{count:,}<br>({percent:.1f}%)" for count, percent in zip(status_counts.values, percentuais)]
    
    # Criar labels em português para o eixo X
    x_labels = [status_portugues.get(status, status) for status in status_counts.index]
    
    fig = go.Figure(data=[go.Bar(
        x=x_labels,
        y=status_counts.values,
        marker_color=bar_colors,
        text=text_values,
        textposition='auto',
        textfont=dict(size=12, color='#333333'),
        hovertemplate='<b>%{x}</b><br>' +
                     'Quantidade: %{y:,}<br>' +
                     'Percentual: %{customdata:.1f}%<br>' +
                     '<extra></extra>',
        customdata=percentuais
    )])
    
    fig.update_layout(
        title={
            'text': "Distribuição de Status das Mensagens",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#2c3e50'}
        },
        xaxis_title="Status",
        yaxis_title="Quantidade de Mensagens",
        height=450,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(color='#333333'),
        xaxis=dict(
            gridcolor='#e9ecef',
            title_font=dict(size=14, color='#2c3e50'),
            tickfont=dict(size=12, color='#2c3e50')
        ),
        yaxis=dict(
            gridcolor='#e9ecef',
            title_font=dict(size=14, color='#2c3e50'),
            tickfont=dict(size=12, color='#2c3e50')
        ),
        showlegend=False  # Não precisa de legenda para gráfico de barras
    )
    
    return fig





def main():
    # Navegação entre páginas
    st.sidebar.markdown("## 📱 Navegação")
    page = st.sidebar.selectbox(
        "Escolha a página:",
        ["🏠 Dashboard Principal", "🔐 Gerenciador de Tokens Facta"],
        index=0
    )
    
    # Renderizar página selecionada
    if page == "🔐 Gerenciador de Tokens Facta":
        render_facta_token_page()
        return
    
    # Dashboard Principal
    # Header principal
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="color: #2c3e50; margin-bottom: 10px;">📱 Servix</h1>
        <p style="color: #7f8c8d; font-size: 16px;">Dashboard de Análise de SMS e Acessos</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Sidebar para configurações
    with st.sidebar:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #2c3e50; margin-bottom: 15px;">⚙️ Configurações</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Status do token FACTA
        st.markdown("### 🔐 Status Token FACTA")
        facta_token = get_facta_token()
        facta_token_valid = is_facta_token_valid()
        
        if facta_token and facta_token_valid:
            st.success("✅ Token FACTA: Ativo")
        elif facta_token and not facta_token_valid:
            st.warning("⚠️ Token FACTA: Expirado")
        else:
            st.error("❌ Token FACTA: Não encontrado")
        
        if st.button("🔄 Atualizar Token FACTA", type="secondary", use_container_width=True):
            st.info("💡 Use a página 'Gerenciador de Tokens Facta' para gerar novos tokens")
        
        st.markdown("---")
        
        token = st.text_input(
            "🔑 Token de Autorização",
            value=DEFAULT_TOKEN,
            type="password",
            help="Token Bearer para autenticação na API."
        )
        
        st.markdown("---")
        st.subheader("📅 Período de Consulta")
        
        end_date_default = datetime.now()
        start_date_default = end_date_default - timedelta(days=7)
        
        start_at_date = st.date_input(
            "📆 Data Inicial",
            value=start_date_default,
            max_value=end_date_default
        )
        
        end_at_date = st.date_input(
            "📆 Data Final",
            value=end_date_default,
            max_value=end_date_default
        )
        
        centro_custo_input = st.text_input(
            "🏢 Centro de Custo (ex: 8103)",
            value="8103",
            help="Filtrar por centro de custo específico."
        )
        
        st.markdown("---")
        # Configurações para acessos de encurtador
        st.subheader("🔗 Acessos de Encurtador")
        
        tenant_segment_id_input = st.number_input(
            "🏢 ID do Centro de Custo (Encurtador)",
            min_value=0,
            value=0,
            help="ID do centro de custo para filtrar acessos (0 = todos)"
        )
        
        is_robot_filter = st.selectbox(
            "🤖 Filtro de Robô",
            options=["Todos", "Não Robô", "Robô"],
            help="Filtrar por tipo de acesso"
        )
        
        limit_accesses = st.number_input(
            "📊 Limite de Registros por Página",
            min_value=1000,
            max_value=5000,
            value=5000,
            help="Registros por página (recomendado: 5000 para buscar todos os dados)"
        )
        
        acessos_unicos = st.checkbox(
            "🔍 Retornar apenas acessos únicos",
            value=True,
            help="Remove duplicatas baseadas em telefone, CPF e centro de custo"
        )
        
                        # Sistema configurado para extrair CPFs dos SMS
        
        st.markdown("---")
        
        # Configuração do token FACTA
        st.subheader("🏦 API FACTA")
        
        # Usar token gerenciado automaticamente
        token_facta_auto = get_facta_token()
        if token_facta_auto and is_facta_token_valid():
            token_facta = token_facta_auto
            st.success("✅ Usando token FACTA gerenciado automaticamente")
        else:
            token_facta = st.text_input(
                "🔑 Token FACTA (Manual)",
                value=FACTA_DEFAULT_TOKEN if FACTA_DEFAULT_TOKEN else "",
                type="password",
                help="Token Bearer para autenticação na API FACTA (use o gerenciador para tokens automáticos)"
            )
            st.warning("⚠️ Token gerenciado não disponível. Use a página 'Gerenciador de Tokens Facta'")
        
        st.markdown("---")
        st.subheader("📁 Importar Dados Locais")
        uploaded_file = st.file_uploader(
            "📂 Carregar arquivo Excel",
            type=['xlsx', 'xls'],
            help="Faça upload de um arquivo Excel para análise adicional e comparação."
        )
        
        if uploaded_file is not None:
            try:
                df_excel = pd.read_excel(uploaded_file)
                st.session_state.df_excel = df_excel
                st.success(f"✅ Arquivo carregado com sucesso! {len(df_excel):,} registros encontrados.")
                
                with st.expander("👀 Visualizar dados do Excel"):
                    st.dataframe(df_excel.head(10), use_container_width=True)
                    
            except Exception as e:
                st.error(f"❌ Erro ao ler arquivo Excel: {str(e)}")
        
        st.markdown("---")
        st.subheader("🚀 Ações")
        
        # Botões organizados verticalmente
        if st.button("🧪 Testar Conexão", type="secondary", use_container_width=True):
            with st.spinner("Testando conexão..."):
                testar_conexao_api(token)
        
        if st.button("🔍 Buscar Dados SMS, Acessos e FACTA", type="primary", use_container_width=True):
            st.session_state.buscar_dados = True
            st.session_state.centro_custo_filtro = centro_custo_input
            st.session_state.start_date_api = start_at_date
            st.session_state.end_date_api = end_at_date
            st.session_state.token_api = token
            
            # Configurações para acessos de encurtador
            st.session_state.buscar_acessos = True
            st.session_state.tenant_segment_id = tenant_segment_id_input
            st.session_state.is_robot_filter = is_robot_filter
            st.session_state.limit_accesses = limit_accesses
            st.session_state.acessos_unicos = acessos_unicos
            st.session_state.start_date_accesses = start_at_date
            st.session_state.end_date_accesses = end_at_date
            st.session_state.token_accesses = token
            
            # Configurações para API FACTA (usando valores padrão)
            st.session_state.buscar_propostas_facta = True
            st.session_state.ambiente_facta = 'producao'
            st.session_state.token_facta = token_facta
            st.session_state.convenio_facta = 3
            st.session_state.averbador_facta = None
            st.session_state.af_facta = None
            st.session_state.data_ini_facta = start_at_date
            st.session_state.data_fim_facta = end_at_date
            st.session_state.data_alteracao_ini_facta = start_at_date
            st.session_state.data_alteracao_fim_facta = end_at_date
            st.session_state.pagina_facta = 1
            st.session_state.quantidade_facta = 1000
            st.session_state.consulta_sub_facta = 'N'
            st.session_state.codigo_sub_facta = None
        
    if 'buscar_dados' in st.session_state and st.session_state.buscar_dados:
        
        start_at_str = f"{st.session_state.start_date_api.strftime('%Y-%m-%d')} 00:00"
        
        # Ajustar data final para não exceder o momento atual
        now = datetime.now()
        if st.session_state.end_date_api > now.date():
            end_at_date_adjusted = now.date()
            st.warning("⚠️ Data final ajustada para hoje (limite da API).")
        else:
            end_at_date_adjusted = st.session_state.end_date_api
        
        if end_at_date_adjusted == now.date():
            end_at_str = now.strftime('%Y-%m-%d %H:%M')
        else:
            end_at_str = f"{end_at_date_adjusted.strftime('%Y-%m-%d')} 23:59"
        
        with st.spinner("Buscando todos os seus envios no período selecionado... Isso pode levar alguns minutos para grandes volumes de dados."):
            df_sms = obter_relatorio_sms_paginado(
                start_at_str,
                end_at_str,
                st.session_state.token_api,
                st.session_state.centro_custo_filtro
            )
        
        if df_sms is None:
            # Sem dados SMS disponíveis
            return
        elif df_sms is not None and not df_sms.empty:
            df_sms = normalizar_status(df_sms)
            st.session_state.df_sms = df_sms 
            
            metricas = calcular_metricas(df_sms)
            st.session_state.metricas = metricas  
            # Seção de métricas principais com melhor alinhamento
            st.markdown("""
            <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
                <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">📊 Resumo Geral</h3>
            </div>
            """, unsafe_allow_html=True)
            
            # Métricas em grid responsivo
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                    <h4 style="color: #2c3e50; margin: 0 0 5px 0;">📱 Total de Mensagens</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{metricas['total_mensagens']:,}"), unsafe_allow_html=True)
            
            with col2:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                    <h4 style="color: #2c3e50; margin: 0 0 5px 0;">✅ Taxa de Sucesso</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{metricas['taxa_sucesso']:.1f}%"), unsafe_allow_html=True)
            
            with col3:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #ffebee; border-radius: 8px; border-left: 4px solid #f44336;">
                    <h4 style="color: #2c3e50; margin: 0 0 5px 0;">❌ Taxa de Falha</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{metricas['taxa_falha']:.1f}%"), unsafe_allow_html=True)
            
            with col4:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                    <h4 style="color: #2c3e50; margin: 0 0 5px 0;">⏳ Pendentes</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{metricas['taxa_pendente']:.1f}%"), unsafe_allow_html=True)
            
            with col5:
                valor_calculado = metricas['total_mensagens'] * 0.0821972734562951
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                    <h4 style="color: #2c3e50; margin: 0 0 5px 0;">💰 Valor Investido</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #2c3e50; margin: 0;">R$ {}</p>
                </div>
                """.format(f"{valor_calculado:,.2f}"), unsafe_allow_html=True)
            

            
            st.markdown("---")
            
            # Seção de análise visual com melhor estrutura
            st.markdown("""
            <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
                <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">📊 Análise Visual</h3>
            </div>
            """, unsafe_allow_html=True)

            # Adicionar métrica de acessos se disponível 
            if 'df_acessos' in st.session_state and st.session_state.df_acessos is not None:
                st.markdown("---")
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                    <h4 style="color: #2c3e50; text-align: center; margin-bottom: 20px;">🔗 Resumo de Acessos de Encurtador</h4>
                </div>
                """, unsafe_allow_html=True)
                
                col_access_summary1, col_access_summary2, col_access_summary3, col_access_summary4 = st.columns(4)
                
                with col_access_summary1:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🔗 Total de Acessos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{len(st.session_state.df_acessos):,}"), unsafe_allow_html=True)
                
                with col_access_summary2:
                    if 'totalAccesses' in st.session_state:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📊 Total API</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{st.session_state.totalAccesses:,}"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📊 Total API</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col_access_summary3:
                    if 'tenant_segment_id' in st.session_state.df_acessos.columns:
                        centros_unicos = st.session_state.df_acessos['tenant_segment_id'].nunique()
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🏢 Centros de Custo</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{centros_unicos}"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🏢 Centros de Custo</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col_access_summary4:
                    if 'is_robot' in st.session_state.df_acessos.columns:
                        acessos_robos = (st.session_state.df_acessos['is_robot'] == '1').sum()
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #fce4ec; border-radius: 8px; border-left: 4px solid #e91e63;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🤖 Acessos de Robôs</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{acessos_robos:,}"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🤖 Acessos de Robôs</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Adicionar métrica de relação entre mensagens e acessos
                st.markdown("---")
                col_relation1, col_relation2, col_relation3 = st.columns(3)
                
                with col_relation1:
                    if 'totalAccesses' in st.session_state:
                        taxa_acesso = (len(st.session_state.df_acessos) / st.session_state.totalAccesses * 100) if st.session_state.totalAccesses > 0 else 0
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📈 Taxa de Acesso</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{taxa_acesso:.1f}%"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📈 Taxa de Acesso</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col_relation2:
                    if 'totalAccesses' in st.session_state:
                        duplicatas = st.session_state.totalAccesses - len(st.session_state.df_acessos)
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #ffebee; border-radius: 8px; border-left: 4px solid #f44336;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🗑️ Duplicatas Removidas</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{duplicatas:,}"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🗑️ Duplicatas Removidas</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
                
                with col_relation3:
                    if 'totalAccesses' in st.session_state:
                        eficiencia = (len(st.session_state.df_acessos) / st.session_state.totalAccesses * 100)
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #e1f5fe; border-radius: 8px; border-left: 4px solid #00bcd4;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">⚡ Eficiência</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                        </div>
                        """.format(f"{eficiencia:.1f}%"), unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                            <h5 style="color: #2c3e50; margin: 0 0 5px 0;">⚡ Eficiência</h5>
                            <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                        </div>
                        """, unsafe_allow_html=True)
            
            # Seletor de tipo de gráfico
            st.markdown("---")
            col_chart_selector1, col_chart_selector2, col_chart_selector3 = st.columns([1, 1, 1])
            with col_chart_selector1:
                tipo_grafico_status = st.selectbox(
                    "📊 Tipo de Gráfico de Status",
                    options=["Pizza (Donut)", "Barras"],
                    help="Escolha o tipo de visualização para a distribuição de status"
                )
            
            # Gráficos em container organizado
            st.markdown("---")
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                st.markdown("""
                <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 20px;">
                """, unsafe_allow_html=True)
                
                if tipo_grafico_status == "Pizza (Donut)":
                    fig_status = criar_grafico_status(df_sms)
                else:
                    fig_status = criar_grafico_barras_status(df_sms)
                
                # Adicionar título com total de acessos se disponível
                if 'df_acessos' in st.session_state and st.session_state.df_acessos is not None:
                    st.markdown(f"#### 📊 Status das Mensagens + 🔗 Total de Acessos: {len(st.session_state.df_acessos):,}")
                else:
                    st.markdown("#### 📊 Status das Mensagens")
                
                st.plotly_chart(fig_status, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col_chart2:
                st.markdown("""
                <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 20px;">
                """, unsafe_allow_html=True)
                
                fig_timeline = criar_grafico_timeline(df_sms)
                st.markdown("#### 📈 Evolução dos Status ao Longo do Tempo")
                st.plotly_chart(fig_timeline, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Informações sobre as legendas
            with st.expander("ℹ️ Informações sobre as Legendas"):
                st.markdown("""
                **📊 Gráfico de Status:**
                - **Verde (#28a745)**: ENTREGUES - Mensagens entregues com sucesso
                - **Azul Claro (#17a2b8)**: NUMEROS INVALIDOS - Mensagens enviadas
                - **Amarelo (#ffc107)**: NÃO ENTREGUES - Mensagens não entregues
                - **Cinza (#6c757d)**: REJEITADO POR BROKER - Mensagens rejeitadas pelo broker
                - **Vermelho (#dc3545)**: FALHA - Mensagens com falha
                - **Azul (#007bff)**: PENDENTES - Mensagens pendentes
                - **Laranja (#fd7e14)**: EM PROCESSAMENTO - Mensagens em processamento
                - **Roxo (#6f42c1)**: AGENDADAS - Mensagens agendadas
                - **Cinza Escuro (#343a40)**: BLOQUEADAS - Mensagens bloqueadas
                - **Rosa (#e83e8c)**: EXPIRADAS - Mensagens expiradas
                
                **📈 Gráfico de Evolução Temporal:**
                - Mostra a evolução da quantidade de mensagens por status ao longo do tempo
                - As legendas incluem o total de mensagens para cada status
                - Passe o mouse sobre os pontos para ver detalhes específicos
                """)
            
            st.markdown("---")
            
            # Seção de dados detalhados com melhor estrutura
            st.markdown("""
            <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
                <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">🔍 Dados Detalhados e Filtros</h3>
            </div>
            """, unsafe_allow_html=True)
            
            # Filtros para a tabela em container organizado
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                <h4 style="color: #2c3e50; margin-bottom: 15px;">🎯 Filtros de Dados</h4>
            </div>
            """, unsafe_allow_html=True)
            
            col_filter1, col_filter2, col_filter3 = st.columns(3)
            
            with col_filter1:
                status_options = sorted(df_sms['status_norm'].dropna().unique())
                status_filter = st.multiselect("📊 Filtrar por Status", options=status_options, default=status_options)
            
            with col_filter2:
                search_term = st.text_input("🔍 Buscar por nome ou telefone", help="Busca case-insensitive em 'nome' e 'telefone'.")
            
            with col_filter3:
                centro_custo_options = []
                if 'centro_custo' in df_sms.columns:
                    centro_custo_options = sorted(df_sms['centro_custo'].dropna().unique())
                    centro_custo_filter = st.multiselect("🏢 Filtrar por Centro de Custo", options=centro_custo_options, default=centro_custo_options)
                else:
                    centro_custo_filter = [] # Garante que seja uma lista vazia se a coluna não existir
            
            df_filtered = df_sms.copy()
            
            # Aplicar filtros
            if status_filter:
                df_filtered = df_filtered[df_filtered['status_norm'].isin(status_filter)]
            
            if centro_custo_filter and 'centro_custo' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['centro_custo'].isin(centro_custo_filter)]
            
            if search_term:
                # Converter colunas para string antes de buscar para evitar erros
                if 'nome' in df_filtered.columns:
                    df_filtered['nome'] = df_filtered['nome'].astype(str)
                if 'telefone' in df_filtered.columns:
                    df_filtered['telefone'] = df_filtered['telefone'].astype(str)
                
                mask_search = (
                    df_filtered.get('nome', pd.Series(dtype=str)).str.contains(search_term, case=False, na=False) |
                    df_filtered.get('telefone', pd.Series(dtype=str)).str.contains(search_term, na=False)
                )
                df_filtered = df_filtered[mask_search]
            
            # Exibir dados filtrados em container organizado
            st.markdown("""
            <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;">
                <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 Dados SMS Filtrados ({})</h4>
            </div>
            """.format(f"{len(df_filtered):,} registros"), unsafe_allow_html=True)
            
            st.dataframe(df_filtered, use_container_width=True, hide_index=True)
    
    # Seção de acessos de encurtador (executada automaticamente com SMS)
    if 'buscar_acessos' in st.session_state and st.session_state.buscar_acessos:
        start_at_accesses = f"{st.session_state.start_date_accesses.strftime('%Y-%m-%d')}"
        
        # Ajustar data final para não exceder o momento atual
        now = datetime.now()
        if st.session_state.end_date_accesses > now.date():
            end_at_accesses_adjusted = now.date()
            st.warning("⚠️ Data final ajustada para hoje (limite da API).")
        else:
            end_at_accesses_adjusted = st.session_state.end_date_accesses
        
        end_at_accesses = f"{end_at_accesses_adjusted.strftime('%Y-%m-%d')}"
        
        # Converter filtro de robô
        is_robot_value = None
        if st.session_state.is_robot_filter == "Não Robô":
            is_robot_value = 0
        elif st.session_state.is_robot_filter == "Robô":
            is_robot_value = 1
        
        with st.spinner("Buscando acessos de encurtador..."):
            df_acessos, total_accesses = obter_acessos_encurtador(
                start_at_accesses,
                end_at_accesses,
                st.session_state.token_accesses,
                st.session_state.tenant_segment_id if st.session_state.tenant_segment_id > 0 else None,
                is_robot_value,
                st.session_state.limit_accesses,
                st.session_state.acessos_unicos
            )
        
        if df_acessos is None:
            st.error("🔑 Token do Kolmeya não configurado ou inválido. Configure o token na seção de configurações.")
            return
        elif df_acessos is not None and not df_acessos.empty:
            st.session_state.df_acessos = df_acessos
            st.session_state.totalAccesses = total_accesses
            
            # Seção de acessos com melhor estrutura
            st.markdown("""
            <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
                <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">🔗 Acessos de Encurtador (Executado Automaticamente)</h3>
            </div>
            """, unsafe_allow_html=True)
            
            # Métricas dos acessos em grid organizado
            col_access1, col_access2, col_access3, col_access4 = st.columns(4)
            
            with col_access1:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🔗 Total de Acessos</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{len(df_acessos):,}"), unsafe_allow_html=True)
            
            with col_access2:
                if 'tenant_segment_id' in df_acessos.columns:
                    centros_unicos = df_acessos['tenant_segment_id'].nunique()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🏢 Centros de Custo</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{centros_unicos}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🏢 Centros de Custo</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_access3:
                if 'is_robot' in df_acessos.columns:
                    acessos_robos = (df_acessos['is_robot'] == '1').sum()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #fce4ec; border-radius: 8px; border-left: 4px solid #e91e63;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🤖 Acessos de Robôs</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{acessos_robos:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🤖 Acessos de Robôs</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_access4:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📊 Total API</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{total_accesses:,}"), unsafe_allow_html=True)
            
            # Segunda linha de métricas
            col_access5, col_access6, col_access7, col_access8 = st.columns(4)
            
            with col_access5:
                if 'accessed_at' in df_acessos.columns:
                    acessos_hoje = df_acessos[df_acessos['accessed_at'].dt.date == datetime.now().date()].shape[0]
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📅 Acessos Hoje</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{acessos_hoje:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📅 Acessos Hoje</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_access6:
                if 'accessed_at' in df_acessos.columns:
                    acessos_ontem = df_acessos[df_acessos['accessed_at'].dt.date == (datetime.now().date() - timedelta(days=1))].shape[0]
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📅 Acessos Ontem</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{acessos_ontem:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📅 Acessos Ontem</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_access7:
                if 'job_id' in df_acessos.columns:
                    jobs_unicos = df_acessos['job_id'].nunique()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e1f5fe; border-radius: 8px; border-left: 4px solid #00bcd4;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📋 Jobs Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{jobs_unicos:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📋 Jobs Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_access8:
                if 'fullphone' in df_acessos.columns:
                    telefones_unicos = df_acessos['fullphone'].nunique()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f3e5f5; border-radius: 8px; border-left: 4px solid #9c27b0;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📱 Telefones Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{telefones_unicos:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📱 Telefones Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Filtros para acessos
            st.markdown("---")
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                <h4 style="color: #2c3e50; margin-bottom: 15px;">🔍 Filtros para Acessos</h4>
            </div>
            """, unsafe_allow_html=True)
            
            col_access_filter1, col_access_filter2, col_access_filter3 = st.columns(3)
            
            with col_access_filter1:
                if 'tenant_segment_id' in df_acessos.columns:
                    centros_options = sorted(df_acessos['tenant_segment_id'].dropna().unique())
                    centro_filter = st.multiselect("🏢 Filtrar por Centro de Custo", options=centros_options, default=centros_options)
                else:
                    centro_filter = []
            
            with col_access_filter2:
                if 'is_robot' in df_acessos.columns:
                    robot_options = sorted(df_acessos['is_robot'].dropna().unique())
                    robot_filter = st.multiselect("🤖 Filtrar por Tipo de Acesso", options=robot_options, default=robot_options)
                else:
                    robot_filter = []
            
            with col_access_filter3:
                search_access = st.text_input("🔍 Buscar por nome ou telefone", help="Busca case-insensitive em 'name' e 'fullphone'.")
            
            # Aplicar filtros
            df_acessos_filtered = df_acessos.copy()
            
            if centro_filter and 'tenant_segment_id' in df_acessos_filtered.columns:
                df_acessos_filtered = df_acessos_filtered[df_acessos_filtered['tenant_segment_id'].isin(centro_filter)]
            
            if robot_filter and 'is_robot' in df_acessos_filtered.columns:
                df_acessos_filtered = df_acessos_filtered[df_acessos_filtered['is_robot'].isin(robot_filter)]
            
            if search_access:
                if 'name' in df_acessos_filtered.columns:
                    df_acessos_filtered['name'] = df_acessos_filtered['name'].astype(str)
                if 'fullphone' in df_acessos_filtered.columns:
                    df_acessos_filtered['fullphone'] = df_acessos_filtered['fullphone'].astype(str)
                
                mask_search_access = (
                    df_acessos_filtered.get('name', pd.Series(dtype=str)).str.contains(search_access, case=False, na=False) |
                    df_acessos_filtered.get('fullphone', pd.Series(dtype=str)).str.contains(search_access, na=False)
                )
                df_acessos_filtered = df_acessos_filtered[mask_search_access]
            
            # Exibir dados filtrados em container organizado
            st.markdown("""
            <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;">
                <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 Acessos Filtrados ({})</h4>
            </div>
            """.format(f"{len(df_acessos_filtered):,} registros"), unsafe_allow_html=True)
            
            st.dataframe(df_acessos_filtered, use_container_width=True, hide_index=True)
            
            # Download dos dados
            if not df_acessos_filtered.empty:
                csv_acessos = df_acessos_filtered.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV - Acessos",
                    data=csv_acessos,
                    file_name=f"acessos_encurtador_{start_at_accesses}_{end_at_accesses}.csv",
                    mime="text/csv"
                )

            # Extração de CPFs dos SMS (funcionando em background)

            # Extrair CPFs dos SMS (que já funcionam)
            if 'df_sms' in st.session_state and st.session_state.df_sms is not None:
                df_sms = st.session_state.df_sms
                
                # Verificar se há coluna de CPF nos SMS
                if 'cpf' in df_sms.columns and df_sms['cpf'].notna().any():
                    # Formatar CPFs dos SMS como strings com 11 dígitos e remover duplicatas
                    cpfs_sms = (
                        df_sms['cpf']
                            .dropna()
                            .astype(str)
                            .str.replace(r'[^\d]', '', regex=True)  # Remover caracteres não numéricos
                            .str.zfill(11)
                    )
                    
                    # Filtrar apenas CPFs válidos (11 dígitos)
                    cpfs_sms = cpfs_sms[cpfs_sms.str.len() == 11]
                    
                    if len(cpfs_sms) > 0:
                        df_cpfs_sms = pd.DataFrame({'cpf': cpfs_sms.drop_duplicates().sort_values().reset_index(drop=True)})
                        
                        st.markdown(f"**📊 CPFs extraídos dos SMS:** {len(df_cpfs_sms):,} CPFs únicos")
                        st.dataframe(df_cpfs_sms.head(20), use_container_width=True, hide_index=True)
                        
                        # Download dos CPFs dos SMS
                        csv_cpfs_sms = df_cpfs_sms.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV - CPFs dos SMS",
                            data=csv_cpfs_sms,
                            file_name=f"cpfs_sms_{start_at_accesses}_{end_at_accesses}.csv",
                            mime="text/csv"
                        )
                        
                        # Configurar automaticamente os CPFs dos SMS para consulta FACTA
                        cpfs_sms_list = df_cpfs_sms['cpf'].tolist()
                        st.session_state.cpfs_para_consultar = cpfs_sms_list
                        # CPFs configurados automaticamente para FACTA (sem interface)
                        
                        # Verificar se há colunas de CPF (sem mostrar na interface)
                        cpf_columns = [col for col in df_sms.columns if 'cpf' in col.lower() or 'documento' in col.lower()]
                        
                        if not cpf_columns:
                            st.error("❌ Nenhuma coluna de CPF encontrada nos SMS")
                            
                else:
                    st.warning("⚠️ Coluna 'cpf' não encontrada nos SMS")
                    # Verificação silenciosa da estrutura dos dados SMS
                    
                    # Mostrar colunas disponíveis nos SMS (apenas se houver erro)
                    if not any(word in col.lower() for word in ['cpf', 'documento', 'identidade', 'rg'] for col in df_sms.columns):
                        st.error("❌ Nenhuma coluna de identificação encontrada nos SMS")
                        
            else:
                st.error("❌ Dados SMS não disponíveis. Execute primeiro a busca de SMS.")
                st.warning("⚠️ Sem dados SMS, não é possível extrair CPFs para filtrar a FACTA")
    
            # CPFs configurados automaticamente para FACTA (sem interface adicional)
        
        # Seção de propostas da FACTA (executada automaticamente com SMS)
        if 'buscar_propostas_facta' in st.session_state and st.session_state.buscar_propostas_facta:
            # Converter datas para formato DD/MM/AAAA
            data_ini_str = st.session_state.data_ini_facta.strftime('%d/%m/%Y')
            data_fim_str = st.session_state.data_fim_facta.strftime('%d/%m/%Y')
            data_alteracao_ini_str = st.session_state.data_alteracao_ini_facta.strftime('%d/%m/%Y')
            data_alteracao_fim_str = st.session_state.data_alteracao_fim_facta.strftime('%d/%m/%Y')
        
        with st.spinner("Buscando propostas da FACTA..."):
            # Verificar se há CPFs válidos dos acessos para filtrar
            cpfs_validos = None
            if 'cpfs_para_consultar' in st.session_state and st.session_state.cpfs_para_consultar:
                cpfs_validos = st.session_state.cpfs_para_consultar
            
            df_propostas = obter_propostas_facta(
                token=st.session_state.token_facta,
                ambiente=st.session_state.ambiente_facta,
                convenio=st.session_state.convenio_facta,
                averbador=st.session_state.averbador_facta,
                af=st.session_state.af_facta,
                data_ini=data_ini_str,
                data_fim=data_fim_str,
                data_alteracao_ini=data_alteracao_ini_str,
                data_alteracao_fim=data_alteracao_fim_str,
                pagina=st.session_state.pagina_facta,
                quantidade=st.session_state.quantidade_facta,
                consulta_sub=st.session_state.consulta_sub_facta,
                codigo_sub=st.session_state.codigo_sub_facta,
                # Passar CPFs válidos para filtragem
                cpfs_validos=cpfs_validos
            )
        
        if df_propostas is not None and not df_propostas.empty:
            st.session_state.df_propostas = df_propostas
            
            # Seção de propostas com melhor estrutura
            st.markdown("""
            <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
                <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">🏦 Propostas FACTA (Executado Automaticamente)</h3>
            </div>
            """, unsafe_allow_html=True)
            
            # Métricas das propostas em grid organizado
            col_prop1, col_prop2, col_prop3, col_prop4, col_prop5 = st.columns(5)
            
            # Filtro de CPFs ativo (sem interface adicional)
            
            with col_prop1:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📊 Total de Propostas</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(f"{len(df_propostas):,}"), unsafe_allow_html=True)
            
            with col_prop2:
                if 'valor_bruto' in df_propostas.columns:
                    valor_total = df_propostas['valor_bruto'].sum()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">💰 Valor Total</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">R$ {}</p>
                    </div>
                    """.format(f"{valor_total:,.2f}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">💰 Valor Total</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_prop3:
                if 'status_proposta' in df_propostas.columns:
                    status_unicos = df_propostas['status_proposta'].nunique()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📋 Status Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{status_unicos}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📋 Status Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_prop4:
                if 'valor_af' in df_propostas.columns:
                    valor_af_total = df_propostas['valor_af'].sum()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f3e5f5; border-radius: 8px; border-left: 4px solid #9c27b0;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">💰 Valor AF Total</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">R$ {}</p>
                    </div>
                    """.format(f"{valor_af_total:,.2f}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">💰 Valor AF Total</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_prop5:
                if 'cpf' in df_propostas.columns:
                    cpfs_unicos = df_propostas['cpf'].nunique()
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e0f2f1; border-radius: 8px; border-left: 4px solid #009688;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">👥 CPFs Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(f"{cpfs_unicos:,}"), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">👥 CPFs Únicos</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Filtros para propostas
            st.markdown("---")
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                <h4 style="color: #2c3e50; margin-bottom: 15px;">🔍 Filtros para Propostas</h4>
            </div>
            """, unsafe_allow_html=True)
            
            col_prop_filter1, col_prop_filter2, col_prop_filter3 = st.columns(3)
            
            with col_prop_filter1:
                if 'status_proposta' in df_propostas.columns:
                    status_options = sorted(df_propostas['status_proposta'].dropna().unique())
                    status_filter_prop = st.multiselect("📋 Filtrar por Status", options=status_options, default=status_options)
                else:
                    status_filter_prop = []
                
                # Filtro para contratos pagos
                mostrar_apenas_pagos = st.checkbox(
                    "💰 Mostrar apenas contratos pagos", 
                    value=False,
                    help="Filtrar apenas contratos com status de pagamento confirmado"
                )
            
            with col_prop_filter2:
                if 'convenio' in df_propostas.columns:
                    convenio_options = sorted(df_propostas['convenio'].dropna().unique())
                    convenio_filter = st.multiselect("🏢 Filtrar por Convênio", options=convenio_options, default=convenio_options)
                else:
                    convenio_filter = []
            
            with col_prop_filter3:
                search_prop = st.text_input("🔍 Buscar por cliente ou CPF", help="Busca case-insensitive em 'cliente' e 'cpf'.")
            
            # Aplicar filtros
            df_propostas_filtered = df_propostas.copy()
            
            if status_filter_prop and 'status_proposta' in df_propostas_filtered.columns:
                df_propostas_filtered = df_propostas_filtered[df_propostas_filtered['status_proposta'].isin(status_filter_prop)]
            
            if convenio_filter and 'convenio' in df_propostas_filtered.columns:
                df_propostas_filtered = df_propostas_filtered[df_propostas_filtered['convenio'].isin(convenio_filter)]
            
            # Filtro para contratos pagos
            if mostrar_apenas_pagos and 'status_proposta' in df_propostas_filtered.columns:
                # Lista de status que indicam contrato pago/efetivado
                status_pagos = [
                    'EFETIVADO', 'PAGO', 'LIQUIDADO', 'CONCLUIDO', 'APROVADO',
                    'EFETIVADA', 'PAGA', 'LIQUIDADA', 'CONCLUIDA', 'APROVADA',
                    'EFETIVADO - PAGO', 'EFETIVADO - LIQUIDADO', 'CONTRATO ATIVO',
                    'ATIVO', 'EM VIGOR', 'VIGENTE', 'FINALIZADO', 'FINALIZADA'
                ]
                
                # Buscar por padrões que indiquem pagamento
                mask_pagos = df_propostas_filtered['status_proposta'].str.contains(
                    '|'.join(status_pagos), 
                    case=False, 
                    na=False
                )
                
                # Também verificar se há data de efetivação ou pagamento
                if 'data_efetivacao' in df_propostas_filtered.columns:
                    mask_efetivacao = df_propostas_filtered['data_efetivacao'].notna()
                    mask_pagos = mask_pagos | mask_efetivacao
                
                if 'data_pgto_cliente' in df_propostas_filtered.columns:
                    mask_pagamento = df_propostas_filtered['data_pgto_cliente'].notna()
                    mask_pagos = mask_pagos | mask_pagamento
                
                df_propostas_filtered = df_propostas_filtered[mask_pagos]
                
                # Mostrar informações sobre o filtro aplicado
                st.info(f"💰 Filtro aplicado: Mostrando apenas contratos pagos/efetivados ({len(df_propostas_filtered):,} de {len(df_propostas):,} propostas)")
            
            if search_prop:
                if 'cliente' in df_propostas_filtered.columns:
                    df_propostas_filtered['cliente'] = df_propostas_filtered['cliente'].astype(str)
                if 'cpf' in df_propostas_filtered.columns:
                    df_propostas_filtered['cpf'] = df_propostas_filtered['cpf'].astype(str)
                
                mask_search_prop = (
                    df_propostas_filtered.get('cliente', pd.Series(dtype=str)).str.contains(search_prop, case=False, na=False) |
                    df_propostas_filtered.get('cpf', pd.Series(dtype=str)).str.contains(search_prop, na=False)
                )
                df_propostas_filtered = df_propostas_filtered[mask_search_prop]
            
            # Exibir dados filtrados em container organizado
            if mostrar_apenas_pagos:
                st.markdown("""
                <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 4px solid #4caf50; margin: 20px 0;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">💰 Contratos Pagos/Efetivados ({})</h4>
                    <p style="color: #7f8c8d; margin: 0;">Exibindo apenas contratos com status de pagamento confirmado</p>
                </div>
                """.format(f"{len(df_propostas_filtered):,} registros"), unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 Propostas Filtradas ({})</h4>
                </div>
                """.format(f"{len(df_propostas_filtered):,} registros"), unsafe_allow_html=True)
            
            st.dataframe(df_propostas_filtered, use_container_width=True, hide_index=True)
            
            # Seção específica para contratos pagos
            if mostrar_apenas_pagos and not df_propostas_filtered.empty:
                st.markdown("---")
                st.markdown("""
                <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 4px solid #4caf50; margin: 20px 0;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">💰 Resumo Financeiro - Contratos Pagos</h4>
                </div>
                """, unsafe_allow_html=True)
                
                col_pagos1, col_pagos2, col_pagos3, col_pagos4 = st.columns(4)
                
                with col_pagos1:
                    valor_total_pagos = df_propostas_filtered['valor_bruto'].sum() if 'valor_bruto' in df_propostas_filtered.columns else 0
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #4caf50; border-radius: 8px; color: white;">
                        <h5 style="margin: 0 0 5px 0;">💰 Valor Total Pago</h5>
                        <p style="font-size: 20px; font-weight: bold; margin: 0;">R$ {}</p>
                    </div>
                    """.format(f"{valor_total_pagos:,.2f}"), unsafe_allow_html=True)
                
                with col_pagos2:
                    valor_af_total_pagos = df_propostas_filtered['valor_af'].sum() if 'valor_af' in df_propostas_filtered.columns else 0
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #2196f3; border-radius: 8px; color: white;">
                        <h5 style="margin: 0 0 5px 0;">🏦 Valor AF Total Pago</h5>
                        <p style="font-size: 20px; font-weight: bold; margin: 0;">R$ {}</p>
                    </div>
                    """.format(f"{valor_af_total_pagos:,.2f}"), unsafe_allow_html=True)
                
                with col_pagos3:
                    valor_medio_pago = df_propostas_filtered['valor_bruto'].mean() if 'valor_bruto' in df_propostas_filtered.columns else 0
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #ff9800; border-radius: 8px; color: white;">
                        <h5 style="margin: 0 0 5px 0;">📊 Valor Médio Pago</h5>
                        <p style="font-size: 20px; font-weight: bold; margin: 0;">R$ {}</p>
                    </div>
                    """.format(f"{valor_medio_pago:,.2f}"), unsafe_allow_html=True)
                
                with col_pagos4:
                    total_contratos_pagos = len(df_propostas_filtered)
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #9c27b0; border-radius: 8px; color: white;">
                        <h5 style="margin: 0 0 5px 0;">📋 Total Contratos Pagos</h5>
                        <p style="font-size: 20px; font-weight: bold; margin: 0;">{}</p>
                    </div>
                    """.format(f"{total_contratos_pagos:,}"), unsafe_allow_html=True)
                
                # Análise de performance dos contratos pagos
                st.markdown("**📈 Análise de Performance - Contratos Pagos**")
                
                col_perf1, col_perf2 = st.columns(2)
                
                with col_perf1:
                    # Top 5 maiores valores pagos
                    if 'valor_bruto' in df_propostas_filtered.columns:
                        top_pagos = df_propostas_filtered.nlargest(5, 'valor_bruto')[['cpf', 'cliente', 'valor_bruto', 'status_proposta']]
                        st.markdown("**🏆 Top 5 Maiores Valores Pagos**")
                        st.dataframe(top_pagos, use_container_width=True, hide_index=True)
                
                with col_perf2:
                    # Distribuição por status dos contratos pagos
                    if 'status_proposta' in df_propostas_filtered.columns:
                        status_dist = df_propostas_filtered['status_proposta'].value_counts().head(10)
                        st.markdown("**📊 Distribuição por Status**")
                        st.dataframe(status_dist.reset_index().rename(columns={'index': 'Status', 'status_proposta': 'Quantidade'}), use_container_width=True, hide_index=True)
            
            # Filtro de CPFs ativo (sem interface adicional)
            
            # Download dos dados
            if not df_propostas_filtered.empty:
                csv_propostas = df_propostas_filtered.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV - Propostas FACTA",
                    data=csv_propostas,
                    file_name=f"propostas_facta_{data_ini_str.replace('/', '')}_{data_fim_str.replace('/', '')}.csv",
                    mime="text/csv"
                )
                
                # Seção: CPFs extraídos das propostas
                st.markdown("---")
                st.markdown("""
                <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">🧾 CPFs extraídos das propostas (únicos)</h4>
                </div>
                """, unsafe_allow_html=True)

                if 'cpf' in df_propostas_filtered.columns and df_propostas_filtered['cpf'].notna().any():
                    # Formatar CPFs como strings com 11 dígitos e remover duplicatas
                    cpfs_series = (
                        df_propostas_filtered['cpf']
                            .dropna()
                            .astype(str)
                            .str.zfill(11)
                    )
                    df_cpfs_prop = pd.DataFrame({'cpf': cpfs_series.drop_duplicates().sort_values().reset_index(drop=True)})

                    st.markdown(f"Quantidade de CPFs únicos: {len(df_cpfs_prop):,}")
                    st.dataframe(df_cpfs_prop, use_container_width=True, hide_index=True)

                    # Download apenas de CPFs das propostas
                    csv_cpfs_prop = df_cpfs_prop.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV - CPFs Propostas",
                        data=csv_cpfs_prop,
                        file_name=f"cpfs_propostas_facta_{data_ini_str.replace('/', '')}_{data_fim_str.replace('/', '')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Nenhum CPF disponível nas propostas filtradas.")
    
    # Seção para consultar CPFs dos acessos na FACTA
    if 'consultar_cpfs_acessos_facta' in st.session_state and st.session_state.consultar_cpfs_acessos_facta:
        st.markdown("---")
        
        # Seção de consulta de CPFs dos acessos
        st.markdown("""
        <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
            <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">🔍 Consulta FACTA por CPFs dos Acessos (Executado Automaticamente)</h3>
        </div>
        """, unsafe_allow_html=True)
        
        if 'cpfs_para_consultar' in st.session_state and st.session_state.cpfs_para_consultar:
            cpfs_para_consultar = st.session_state.cpfs_para_consultar
            
            st.info(f"📊 Consultando {len(cpfs_para_consultar):,} CPFs únicos dos acessos na API FACTA")
            
            # Executar consulta automaticamente usando configurações da sidebar
            with st.spinner(f"Consultando {len(cpfs_para_consultar):,} CPFs na API FACTA..."):
                todas_propostas = []
                
                # Progress bar para acompanhar o progresso
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, cpf in enumerate(cpfs_para_consultar):
                    status_text.text(f"Consultando CPF {i+1}/{len(cpfs_para_consultar)}: {cpf}")
                    progress_bar.progress((i + 1) / len(cpfs_para_consultar))
                    
                    try:
                        # Consultar propostas para este CPF usando configurações da sidebar
                        df_proposta_cpf = obter_propostas_facta(
                            token=st.session_state.get('token_facta', FACTA_DEFAULT_TOKEN if FACTA_DEFAULT_TOKEN else ""),
                            ambiente=st.session_state.get('ambiente_facta', 'producao'),
                            convenio=st.session_state.get('convenio_facta', 3) if st.session_state.get('convenio_facta', 3) > 0 else None,
                            quantidade=st.session_state.get('quantidade_facta', 100),
                            # Filtrar por CPF específico
                            cpf=cpf,
                            # Passar lista de CPFs válidos para filtragem
                            cpfs_validos=st.session_state.cpfs_para_consultar
                        )
                        
                        if df_proposta_cpf is not None and not df_proposta_cpf.empty:
                            # Adicionar CPF consultado como identificador
                            df_proposta_cpf['cpf_consultado'] = cpf
                            todas_propostas.append(df_proposta_cpf)
                        
                        # Pequena pausa para não sobrecarregar a API
                        import time
                        time.sleep(0.1)
                        
                    except Exception as e:
                        st.warning(f"⚠️ Erro ao consultar CPF {cpf}: {str(e)}")
                        continue
                
                progress_bar.empty()
                status_text.empty()
                
                if todas_propostas:
                    # Combinar todas as propostas encontradas
                    df_propostas_combinadas = pd.concat(todas_propostas, ignore_index=True)
                    st.session_state.df_propostas_cpfs_acessos = df_propostas_combinadas
                    
                    st.success(f"✅ Consulta concluída! {len(df_propostas_combinadas):,} propostas encontradas para {len(cpfs_para_consultar):,} CPFs")
                    
                    # Exibir resultados
                    st.markdown("---")
                    st.markdown("**📊 Resultados da Consulta**")
                    
                    # Métricas
                    col_res1, col_res2, col_res3, col_res4 = st.columns(4)
                    
                    with col_res1:
                        st.metric("Total de Propostas", f"{len(df_propostas_combinadas):,}")
                    
                    with col_res2:
                        cpfs_com_propostas = df_propostas_combinadas['cpf'].nunique()
                        st.metric("CPFs com Propostas", f"{cpfs_com_propostas:,}")
                    
                    with col_res3:
                        if 'valor_bruto' in df_propostas_combinadas.columns:
                            valor_total = df_propostas_combinadas['valor_bruto'].sum()
                            st.metric("Valor Total", f"R$ {valor_total:,.2f}")
                        else:
                            st.metric("Valor Total", "N/A")
                    
                    with col_res4:
                        if 'valor_af' in df_propostas_combinadas.columns:
                            valor_af_total = df_propostas_combinadas['valor_af'].sum()
                            st.metric("Valor AF Total", f"R$ {valor_af_total:,.2f}")
                        else:
                            st.metric("Valor AF Total", "N/A")
                    
                    # Tabela de resultados
                    st.dataframe(df_propostas_combinadas, use_container_width=True, hide_index=True)
                    
                    # Seção de análise da filtragem
                    if 'cpfs_para_consultar' in st.session_state and st.session_state.cpfs_para_consultar:
                        st.markdown("---")
                        st.markdown("""
                        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                            <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 Análise da Filtragem por CPFs dos Acessos</h4>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Métricas de filtragem
                        col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
                        
                        with col_filtro1:
                            cpfs_acessos = len(st.session_state.cpfs_para_consultar)
                            st.metric("CPFs dos Acessos", f"{cpfs_acessos:,}")
                        
                        with col_filtro2:
                            cpfs_encontrados = df_propostas_combinadas['cpf'].nunique()
                            st.metric("CPFs Encontrados na Facta", f"{cpfs_encontrados:,}")
                        
                        with col_filtro3:
                            taxa_cobertura = (cpfs_encontrados / cpfs_acessos * 100) if cpfs_acessos > 0 else 0
                            st.metric("Taxa de Cobertura", f"{taxa_cobertura:.1f}%")
                        
                        # Gráfico de comparação
                        if cpfs_acessos > 0:
                            st.markdown("**📈 Comparação: CPFs dos Acessos vs. CPFs Encontrados na Facta**")
                            
                            # Criar dados para o gráfico
                            labels = ['CPFs Encontrados', 'CPFs Não Encontrados']
                            sizes = [cpfs_encontrados, cpfs_acessos - cpfs_encontrados]
                            colors = ['#4caf50', '#f44336']
                            
                            # Gráfico de pizza simples
                            fig, ax = plt.subplots(figsize=(8, 6))
                            ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
                            ax.axis('equal')
                            st.pyplot(fig)
                    
                    # Seção destacada para Valor AF Total
                    if 'valor_af' in df_propostas_combinadas.columns:
                        st.markdown("---")
                        st.markdown("""
                        <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 4px solid #4caf50; margin: 20px 0;">
                            <h4 style="color: #2c3e50; margin-bottom: 15px;">💰 Resumo Financeiro - Valor AF</h4>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        col_af1, col_af2, col_af3 = st.columns(3)
                        
                        with col_af1:
                            valor_af_total = df_propostas_combinadas['valor_af'].sum()
                            st.markdown("""
                            <div style="text-align: center; padding: 20px; background-color: #4caf50; border-radius: 8px; color: white;">
                                <h3 style="margin: 0 0 10px 0;">Valor AF Total</h3>
                                <h2 style="margin: 0; font-size: 28px;">R$ {}</h2>
                            </div>
                            """.format(f"{valor_af_total:,.2f}"), unsafe_allow_html=True)
                        
                        with col_af2:
                            valor_af_medio = df_propostas_combinadas['valor_af'].mean()
                            st.markdown("""
                            <div style="text-align: center; padding: 20px; background-color: #2196f3; border-radius: 8px; color: white;">
                                <h3 style="margin: 0 0 10px 0;">Valor AF Médio</h3>
                                <h2 style="margin: 0; font-size: 28px;">R$ {}</h2>
                            </div>
                            """.format(f"{valor_af_medio:,.2f}"), unsafe_allow_html=True)
                        
                        with col_af3:
                            propostas_com_af = df_propostas_combinadas['valor_af'].notna().sum()
                            total_propostas = len(df_propostas_combinadas)
                            st.markdown("""
                            <div style="text-align: center; padding: 20px; background-color: #ff9800; border-radius: 8px; color: white;">
                                <h3 style="margin: 0 0 10px 0;">Propostas com AF</h3>
                                <h2 style="margin: 0; font-size: 28px;">{}/{}</h2>
                                <p style="margin: 5px 0 0 0;">({:.1%})</p>
                            </div>
                            """.format(propostas_com_af, total_propostas, propostas_com_af/total_propostas), unsafe_allow_html=True)
                        
                        # Análise detalhada do valor AF
                        st.markdown("**📊 Análise Detalhada do Valor AF**")
                        
                        col_af_analise1, col_af_analise2 = st.columns(2)
                        
                        with col_af_analise1:
                            # Top 5 maiores valores AF
                            top_af = df_propostas_combinadas.nlargest(5, 'valor_af')[['cpf', 'cliente', 'valor_af', 'status_proposta']]
                            st.markdown("**🏆 Top 5 Maiores Valores AF**")
                            st.dataframe(top_af, use_container_width=True, hide_index=True)
                        
                        with col_af_analise2:
                            # Distribuição por faixas de valor AF
                            if 'valor_af' in df_propostas_combinadas.columns and df_propostas_combinadas['valor_af'].notna().any():
                                df_af_valido = df_propostas_combinadas[df_propostas_combinadas['valor_af'].notna()]
                                
                                # Criar faixas de valor AF
                                faixas_af = pd.cut(df_af_valido['valor_af'], 
                                                 bins=[0, 1000, 5000, 10000, 50000, float('inf')],
                                                 labels=['Até R$ 1k', 'R$ 1k-5k', 'R$ 5k-10k', 'R$ 10k-50k', 'Acima R$ 50k'])
                                
                                df_faixas = faixas_af.value_counts().reset_index()
                                df_faixas.columns = ['Faixa de Valor AF', 'Quantidade']
                                
                                st.markdown("**📈 Distribuição por Faixas de Valor AF**")
                                st.dataframe(df_faixas, use_container_width=True, hide_index=True)
                    
                    # Download dos resultados
                    csv_resultado = df_propostas_combinadas.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV - Resultados Consulta CPFs",
                        data=csv_resultado,
                        file_name=f"consulta_cpfs_acessos_facta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                    # Análise de CPFs encontrados vs não encontrados
                    cpfs_encontrados = set(df_propostas_combinadas['cpf'].unique())
                    cpfs_nao_encontrados = set(cpfs_para_consultar) - cpfs_encontrados
                    
                    if cpfs_nao_encontrados:
                        st.markdown("---")
                        st.markdown("**📋 CPFs sem Propostas Encontradas**")
                        df_cpfs_nao_encontrados = pd.DataFrame({'cpf': sorted(list(cpfs_nao_encontrados))})
                        st.dataframe(df_cpfs_nao_encontrados, use_container_width=True, hide_index=True)
                        
                        # Download CPFs não encontrados
                        csv_nao_encontrados = df_cpfs_nao_encontrados.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV - CPFs sem Propostas",
                            data=csv_nao_encontrados,
                            file_name=f"cpfs_sem_propostas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                else:
                    st.warning("⚠️ Nenhuma proposta encontrada para os CPFs consultados.")
        else:
            st.error("❌ Nenhum CPF disponível para consulta. Execute primeiro a busca de acessos.")
    
                               
    # Seção de dados do Excel (quando disponível)
    if 'df_excel' in st.session_state and st.session_state.df_excel is not None:
        st.markdown("---")
        
        # Seção de dados do Excel com melhor estrutura
        st.markdown("""
        <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); margin: 20px 0;">
            <h3 style="color: #2c3e50; text-align: center; margin-bottom: 25px;">📊 Dados Importados do Excel (Análise Local)</h3>
        </div>
        """, unsafe_allow_html=True)
        
        df_excel = st.session_state.df_excel
        
        # Métricas do Excel em grid organizado
        col_excel_metrics1, col_excel_metrics2, col_excel_metrics3 = st.columns(3)
        with col_excel_metrics1:
            st.markdown("""
            <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📊 Total de Registros</h5>
                <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
            </div>
            """.format(f"{len(df_excel):,}"), unsafe_allow_html=True)
        with col_excel_metrics2:
            st.markdown("""
            <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                <h5 style="color: #2c3e50; margin: 0 0 5px 0;">📋 Colunas</h5>
                <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
            </div>
            """.format(f"{len(df_excel.columns)}"), unsafe_allow_html=True)
        with col_excel_metrics3:
            st.markdown("""
            <div style="text-align: center; padding: 15px; background-color: #ffebee; border-radius: 8px; border-left: 4px solid #f44336;">
                <h5 style="color: #2c3e50; margin: 0 0 5px 0;">🔄 Registros Duplicados</h5>
                <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
            </div>
            """.format(f"{df_excel.duplicated().sum():,}"), unsafe_allow_html=True)
        
        # Filtros para dados do Excel
        st.markdown("---")
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h4 style="color: #2c3e50; margin-bottom: 15px;">🔍 Filtros para Dados do Excel</h4>
        </div>
        """, unsafe_allow_html=True)
        
        col_excel_filter1, col_excel_filter2 = st.columns(2)
        
        df_excel_filtered = df_excel.copy()
        
        with col_excel_filter1:
            if not df_excel.columns.empty:
                coluna_filtro_excel = st.selectbox(
                    "📋 Selecionar coluna para filtrar (Excel)",
                    options=df_excel.columns,
                    key="excel_col_filter_select"
                )
                if coluna_filtro_excel:
                    valores_unicos_excel = sorted(df_excel[coluna_filtro_excel].dropna().astype(str).unique())
                    if len(valores_unicos_excel) <= 50: # Limite para multiselect
                        filtro_valores_excel = st.multiselect(
                            f"🎯 Filtrar por {coluna_filtro_excel} (Excel)",
                            options=valores_unicos_excel,
                            default=valores_unicos_excel,
                            key="excel_val_filter_multiselect"
                        )
                    else:
                        filtro_valores_excel = st.multiselect(
                            f"🎯 Filtrar por {coluna_filtro_excel} (Excel)",
                            options=valores_unicos_excel[:50],
                            help=f"Mostrando apenas os primeiros 50 valores de {coluna_filtro_excel}.",
                            key="excel_val_filter_multiselect_large"
                        )
                    if filtro_valores_excel:
                        df_excel_filtered = df_excel_filtered[df_excel_filtered[coluna_filtro_excel].astype(str).isin(filtro_valores_excel)]
        
        with col_excel_filter2:
            busca_excel = st.text_input("🔍 Buscar em todas as colunas (Excel)", help="Busca case-insensitive em todas as colunas de texto.", key="excel_search_input")
            if busca_excel:
                mask_excel_search = pd.DataFrame([
                    df_excel_filtered[col].astype(str).str.contains(busca_excel, case=False, na=False)
                    for col in df_excel_filtered.columns
                ]).any(axis=0)
                df_excel_filtered = df_excel_filtered[mask_excel_search]
        
        # Exibir dados filtrados em container organizado
        st.markdown("""
        <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;">
            <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 Dados do Excel Filtrados ({})</h4>
        </div>
        """.format(f"{len(df_excel_filtered):,} registros"), unsafe_allow_html=True)
        
        st.dataframe(df_excel_filtered, use_container_width=True, hide_index=True)
        
    else:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 30px; border-radius: 12px; text-align: center; margin: 50px 0;">
            <h3 style="color: #2c3e50; margin-bottom: 15px;">🚀 Bem-vindo ao Servix Dashboard</h3>
            <p style="color: #7f8c8d; font-size: 16px; margin-bottom: 20px;">
                Configure os parâmetros na barra lateral e clique em 'Buscar Dados SMS, Acessos e FACTA' para análise completa automática 
                ou 'Carregar arquivo Excel' para análise local.
            </p>
            <div style="display: flex; justify-content: center; gap: 20px; margin-top: 20px; flex-wrap: wrap;">
                <div style="background-color: #e8f5e9; padding: 15px; border-radius: 8px; border-left: 4px solid #4caf50; min-width: 200px;">
                    <h4 style="color: #2c3e50; margin: 0 0 10px 0;">📱 SMS</h4>
                    <p style="color: #7f8c8d; margin: 0;">Análise de status e métricas</p>
                </div>
                <div style="background-color: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 4px solid #2196f3; min-width: 200px;">
                    <h4 style="color: #2c3e50; margin: 0 0 10px 0;">🔗 Acessos</h4>
                    <p style="color: #7f8c8d; margin: 0;">Análise de encurtadores</p>
                </div>
                <div style="background-color: #fce4ec; padding: 15px; border-radius: 8px; border-left: 4px solid #e91e63; min-width: 200px;">
                    <h4 style="color: #2c3e50; margin: 0 0 10px 0;">🏦 FACTA</h4>
                    <p style="color: #7f8c8d; margin: 0;">Andamento de propostas</p>
                </div>
                <div style="background-color: #fff3e0; padding: 15px; border-radius: 8px; border-left: 4px solid #ff9800; min-width: 200px;">
                    <h4 style="color: #2c3e50; margin: 0 0 10px 0;">📊 Excel</h4>
                    <p style="color: #7f8c8d; margin: 0;">Importação de dados locais</p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()