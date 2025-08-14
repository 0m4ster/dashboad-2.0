import os
import requests
import streamlit as st
from datetime import datetime, timedelta, date
import re
import pandas as pd
import io
import json
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from requests.adapters import HTTPAdapter
import ssl

# Importar gerenciador de banco de dados
try:
    from database_manager import DashboardDatabase, salvar_metricas_dashboard
    HAS_DATABASE = True
except ImportError:
    HAS_DATABASE = False
    print("⚠️ Módulo de banco de dados não encontrado. As métricas não serão salvas.")

try:
    from streamlit_extras.streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# Configuração da API Kolmeya

KOLMEYA_TOKEN_DIRETO = ""  # Coloque seu token aqui para testes

# Função para obter o token da API
def get_kolmeya_token():
    """Retorna o token da API do Kolmeya."""
    print(f"🔍 Buscando token do Kolmeya...")
    
    # Primeiro tenta variável de ambiente
    token = os.environ.get("KOLMEYA_TOKEN", "")
    if token:
        print(f"✅ Token encontrado na variável de ambiente: {token[:10]}...")
        return token
    
    # Se não encontrar, tenta configuração direta
    if KOLMEYA_TOKEN_DIRETO:
        token = KOLMEYA_TOKEN_DIRETO
        print(f"⚠️ Usando token configurado diretamente no código: {token[:10]}...")
        return token
    
    # Se não encontrar, tenta ler do arquivo
    try:
        with open("kolmeya_token.txt", "r") as f:
            token = f.read().strip()
            print(f"✅ Token lido do arquivo kolmeya_token.txt: {token[:10]}...")
            return token
    except FileNotFoundError:
        print("❌ Arquivo kolmeya_token.txt não encontrado")
    except Exception as e:
        print(f"❌ Erro ao ler token do arquivo: {e}")
    
    print("❌ Nenhum token do Kolmeya encontrado")
    return ""

# Função para obter o token da API da Facta
def get_facta_token():
    """Retorna o token da API da Facta."""
    # Primeiro tenta variável de ambiente
    token = os.environ.get("FACTA_TOKEN", "")
    
    # Se não encontrar, tenta ler do arquivo
    if not token:
        try:
            with open("facta_token.txt", "r") as f:
                token = f.read().strip()
                print("✅ Token da Facta lido do arquivo")
        except FileNotFoundError:
            print("❌ Arquivo facta_token.txt não encontrado")
        except Exception as e:
            print(f"❌ Erro ao ler token da Facta: {e}")
    
    return token

# Configurações
CUSTO_POR_ENVIO = 0.08  # R$ 0,08 por SMS

# Constantes para os centros de custo do Kolmeya
TENANT_SEGMENT_ID_FGTS = "FGTS"  # FGTS conforme registro
TENANT_SEGMENT_ID_CLT = "Crédito CLT"   # CRÉDITO CLT conforme registro
TENANT_SEGMENT_ID_NOVO = "Novo"  # NOVO conforme registro

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

def limpar_cpf(cpf):
    """Limpa e valida CPF, preservando zeros à esquerda quando necessário."""
    if not cpf:
        return ""
    
    # Remove caracteres não numéricos
    cpf_limpo = re.sub(r'\D', '', str(cpf))
    
    # Se tem exatamente 11 dígitos, retorna como está
    if len(cpf_limpo) == 11:
        return cpf_limpo
    
    # Se tem menos de 11 dígitos, adiciona zeros à esquerda
    if len(cpf_limpo) < 11:
        return cpf_limpo.zfill(11)
    
    # Se tem mais de 11 dígitos, pega os 11 últimos
    if len(cpf_limpo) > 11:
        return cpf_limpo[-11:]
    
    return ""

def validar_cpf(cpf):
    """Valida se um CPF é válido (algoritmo de validação)."""
    if not cpf or len(cpf) != 11:
        return False
    
    # Verifica se todos os dígitos são iguais (CPF inválido)
    if cpf == cpf[0] * 11:
        return False
    
    # Calcula os dígitos verificadores
    soma = 0
    for i in range(9):
        soma += int(cpf[i]) * (10 - i)
    
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto
    
    soma = 0
    for i in range(10):
        soma += int(cpf[i]) * (11 - i)
    
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto
    
    return cpf[-2:] == f"{digito1}{digito2}"



def extrair_telefones_da_base(df, data_ini=None, data_fim=None):
    """Extrai e limpa todos os números de telefone da base carregada, opcionalmente filtrados por data."""
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
    
    # Procura por colunas de data
    colunas_data = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['data', 'date', 'criacao', 'created', 'timestamp']):
            colunas_data.append(col)
    
    for idx, row in df.iterrows():
        # Verifica se está no período de data (se especificado)
        if data_ini and data_fim and colunas_data:
            data_valida = False
            for col in colunas_data:
                try:
                    data_str = str(row[col])
                    if pd.notna(data_str) and data_str.strip():
                        # Tenta diferentes formatos de data
                        data_criacao = None
                        
                        # Formato: DD/MM/YYYY HH:MM
                        if len(data_str) >= 16 and '/' in data_str:
                            data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M')
                        # Formato: DD/MM/YYYY
                        elif len(data_str) == 10 and '/' in data_str:
                            data_criacao = datetime.strptime(data_str, '%d/%m/%Y')
                        # Formato: YYYY-MM-DD HH:MM:SS
                        elif len(data_str) >= 19:
                            data_criacao = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
                        # Formato: YYYY-MM-DD
                        elif len(data_str) == 10:
                            data_criacao = datetime.strptime(data_str, '%Y-%m-%d')
                        
                        if data_criacao:
                            data_ini_dt = datetime.combine(data_ini, datetime.min.time())
                            data_fim_dt = datetime.combine(data_fim, datetime.max.time())
                            if data_ini_dt <= data_criacao <= data_fim_dt:
                                data_valida = True
                                break
                except (ValueError, TypeError):
                    continue
            
            # Se não está no período, pula este registro
            if not data_valida:
                continue
        
        # Extrai telefones das colunas
        for col in colunas_telefone:
            valor = row[col] if col in row else None
            if valor is not None:
                telefone_limpo = limpar_telefone(valor)
                if telefone_limpo and len(telefone_limpo) == 11:
                    telefones.add(telefone_limpo)
    
    return telefones

def extrair_telefones_kolmeya(messages):
    """Extrai e limpa todos os números de telefone das mensagens do Kolmeya."""
    telefones = set()
    
    for msg in messages:
        if isinstance(msg, dict):
            # Campo 'telefone' da nova API
            if 'telefone' in msg and msg['telefone'] is not None:
                valor_str = str(msg['telefone']).strip()
                telefone_limpo = limpar_telefone(valor_str)
                if telefone_limpo and len(telefone_limpo) == 11:
                    telefones.add(telefone_limpo)
    
    return telefones

def extrair_cpfs_kolmeya(messages):
    """Extrai e limpa todos os CPFs das mensagens do Kolmeya."""
    cpfs = set()
    
    for msg in messages:
        if isinstance(msg, dict):
            # Campo 'cpf' da nova API
            if 'cpf' in msg and msg['cpf'] is not None:
                valor_str = str(msg['cpf']).strip()
                
                # Usar a nova função de limpeza de CPF
                cpf_limpo = limpar_cpf(valor_str)
                if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                        cpfs.add(cpf_limpo)
    
    return cpfs

def extrair_cpfs_da_base(df, data_ini=None, data_fim=None):
    """Extrai e limpa todos os CPFs da base carregada, opcionalmente filtrados por data."""
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
    
    # Procura por colunas de data
    colunas_data = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['data', 'date', 'criacao', 'created', 'timestamp']):
            colunas_data.append(col)
    
    for idx, row in df.iterrows():
        # Verifica se está no período de data (se especificado)
        if data_ini and data_fim and colunas_data:
            data_valida = False
            for col in colunas_data:
                try:
                    data_str = str(row[col])
                    if pd.notna(data_str) and data_str.strip():
                        # Tenta diferentes formatos de data
                        data_criacao = None
                        
                        # Formato: DD/MM/YYYY HH:MM
                        if len(data_str) >= 16 and '/' in data_str:
                            data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M')
                        # Formato: DD/MM/YYYY
                        elif len(data_str) == 10 and '/' in data_str:
                            data_criacao = datetime.strptime(data_str, '%d/%m/%Y')
                        # Formato: YYYY-MM-DD HH:MM:SS
                        elif len(data_str) >= 19:
                            data_criacao = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
                        # Formato: YYYY-MM-DD
                        elif len(data_str) == 10:
                            data_criacao = datetime.strptime(data_str, '%Y-%m-%d')
                        
                        if data_criacao:
                            data_ini_dt = datetime.combine(data_ini, datetime.min.time())
                            data_fim_dt = datetime.combine(data_fim, datetime.max.time())
                            if data_ini_dt <= data_criacao <= data_fim_dt:
                                data_valida = True
                                break
                except (ValueError, TypeError):
                    continue
            
            # Se não está no período, pula este registro
            if not data_valida:
                continue
        
        # Extrai CPFs das colunas
        for col in colunas_cpf:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip()
                
                # Usar a nova função de limpeza de CPF
                cpf_limpo = limpar_cpf(valor_str)
                if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
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
    """Formata um valor numérico para formato de moeda brasileira."""
    try:
        # Converte para float se for string ou outro tipo
        if isinstance(valor, str):
            # Remove caracteres não numéricos exceto ponto e vírgula
            valor_limpo = valor.replace('R$', '').replace(' ', '').strip()
            # Substitui vírgula por ponto para conversão
            valor_limpo = valor_limpo.replace(',', '.')
            valor = float(valor_limpo)
        else:
            valor = float(valor)
        
        # Formata o valor
        return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    except (ValueError, TypeError):
        # Se não conseguir converter, retorna o valor original
        return str(valor)

def obter_saldo_kolmeya(token=None):
    """Retorna saldo real do Kolmeya via API."""
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado para consulta de saldo")
        return 0.0
    
    try:
        url = "https://kolmeya.com.br/api/v1/account/balance"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        print(f"🔍 Consultando saldo Kolmeya:")
        print(f"   🌐 URL: {url}")
        print(f"   🔑 Token: {token[:10]}..." if token else "   🔑 Token: Não fornecido")
        
        resp = requests.get(url, headers=headers, timeout=30)
        
        print(f"   📊 Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"   📄 Resposta: {data}")
            
            # Tentar diferentes campos possíveis para o saldo
            saldo = None
            if 'balance' in data:
                saldo = data.get("balance")
            elif 'saldo' in data:
                saldo = data.get("saldo")
            elif 'amount' in data:
                saldo = data.get("amount")
            elif 'value' in data:
                saldo = data.get("value")
            else:
                print(f"   ⚠️ Campo de saldo não encontrado. Campos disponíveis: {list(data.keys())}")
                saldo = 0.0
            
            saldo_float = float(saldo) if saldo is not None else 0.0
            print(f"   💰 Saldo encontrado: R$ {saldo_float:,.2f}")
            return saldo_float
        else:
            print(f"   ❌ Erro HTTP {resp.status_code}")
            print(f"   📄 Resposta de erro: {resp.text}")
            return 0.0
            
    except requests.exceptions.Timeout:
        print("   ❌ Timeout na requisição de saldo")
        return 0.0
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro na requisição de saldo: {e}")
        return 0.0
    except Exception as e:
        print(f"   ❌ Erro inesperado ao consultar saldo: {e}")
        return 0.0

def obter_dados_sms_com_filtro(data_ini, data_fim, tenant_segment_id=None):
    """Consulta o endpoint Kolmeya para status de SMS."""
    if data_ini is None or data_fim is None:
        return [], 0
    
    # Formatar datas para o formato esperado pela API
    start_at = data_ini.strftime('%Y-%m-%d 00:00')
    
    # CORREÇÃO: Usar timezone brasileiro para garantir que a data seja sempre do Brasil
    from datetime import timezone, timedelta
    tz_brasil = timezone(timedelta(hours=-3))
    agora_brasil = datetime.now(tz_brasil)
    
    # Se a data final for hoje (no Brasil), usar o horário atual para pegar dados em tempo real
    if data_fim == agora_brasil.date():
        end_at = agora_brasil.strftime('%Y-%m-%d %H:%M')
        print(f"🔍 DEBUG - Data final é hoje (BR), usando horário atual: {end_at}")
    else:
        end_at = data_fim.strftime('%Y-%m-%d 23:59')
        print(f"🔍 DEBUG - Data final não é hoje (BR), usando 23:59: {end_at}")
    
    print(f"🔍 Consultando API real do Kolmeya:")
    print(f"   📅 Período: {start_at} a {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    print(f"   🕐 Horário atual (BR): {agora_brasil.strftime('%Y-%m-%d %H:%M')}")
    print(f"   🌍 Fuso horário: UTC-3 (Brasil)")
    
    # Consulta real à API
    try:
        messages = consultar_status_sms_kolmeya(start_at, end_at, token=None, tenant_segment_id=tenant_segment_id)
        
        if messages:
            print(f"✅ API retornou {len(messages)} mensagens")
            # Retornar dados reais sem estimativas
            total_acessos = len(messages)  # Um acesso por SMS
            return messages, total_acessos
        else:
            print("⚠️ API não retornou mensagens")
            return [], 0
            
    except Exception as e:
        print(f"❌ Erro na consulta: {e}")
        return [], 0

def consultar_status_sms_kolmeya(start_at, end_at, limit=30000, token=None, tenant_segment_id=None):
    """Consulta o status das mensagens SMS en runviadas via Kolmeya."""
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado")
        return []
    
    # Verificar se o período não excede 7 dias
    try:
        start_dt = datetime.strptime(start_at, '%Y-%m-%d %H:%M')
        end_dt = datetime.strptime(end_at, '%Y-%m-%d %H:%M')
        diff_days = (end_dt - start_dt).days
        
        if diff_days > 7:
            print(f"❌ Período máximo permitido é de 7 dias. Período solicitado: {diff_days} dias")
            return []
    except ValueError as e:
        print(f"❌ Erro ao converter datas: {e}")
        return []
    
    url = "https://kolmeya.com.br/api/v1/sms/reports/statuses"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    body = {
        "start_at": start_at,
        "end_at": end_at,
        "limit": min(limit, 30000)  # Máximo permitido pela API
    }
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            messages = data.get("messages", [])
            
            print(f"✅ Resposta recebida: {len(messages)} mensagens")
            
            # Debug: Verificar detalhes da resposta
            if messages and len(messages) > 0:
                print(f"🔍 DEBUG - Detalhes da resposta da API:")
                print(f"   📅 Período consultado: {start_at} a {end_at}")
                print(f"   📊 Total de mensagens retornadas: {len(messages)}")
                print(f"   📅 Primeira mensagem - enviada_em: {messages[0].get('enviada_em', 'N/A')}")
                print(f"   📅 Última mensagem - enviada_em: {messages[-1].get('enviada_em', 'N/A')}")
                print(f"   🏢 Centro de custo da primeira: {messages[0].get('centro_custo', 'N/A')}")
                print(f"   📋 Status da primeira: {messages[0].get('status', 'N/A')}")
            else:
                print(f"⚠️ DEBUG - Nenhuma mensagem retornada para o período: {start_at} a {end_at}")
            
            # Filtrar por centro de custo se especificado
            if tenant_segment_id and messages:
                messages_filtradas = []
                
                for msg in messages:
                    if isinstance(msg, dict):
                        # Tentar diferentes campos que podem conter o centro de custo
                        centro_custo_msg = None
                        
                        # Lista de campos possíveis para centro de custo
                        campos_possiveis = [
                            'centro_custo', 'tenant_segment_id', 'cost_center', 'segment',
                            'campaign_id', 'campaign_name', 'template_id', 'template_name',
                            'sender_id', 'sender_name', 'account_id', 'account_name',
                            'group_id', 'group_name', 'tag', 'tags', 'category'
                        ]
                        
                        for campo in campos_possiveis:
                            if campo in msg:
                                valor = msg.get(campo, '')
                                # Se encontrou um valor, tentar usar para filtragem
                                if not centro_custo_msg and valor:
                                    centro_custo_msg = str(valor)
                        
                        # Se encontrou algum campo, verificar se corresponde ao filtro
                        if centro_custo_msg:
                            # Mapear IDs para nomes se necessário
                            mapeamento_centros = {
                                "8105": ["Novo", "8105", "NOVO", "novo", "INSS", "inss", "Inss"],
                                "8103": ["FGTS", "8103", "fgts", "Fgts", "Fgts", "Fgts"], 
                                "8208": ["Crédito CLT", "8208", "CLT", "clt", "Crédito", "CREDITO", "credito", "CLT", "clt"]
                            }
                            
                            valores_aceitos = mapeamento_centros.get(tenant_segment_id, [tenant_segment_id])
                            
                            # Verificar se o valor encontrado corresponde ao filtro
                            if centro_custo_msg in valores_aceitos:
                                messages_filtradas.append(msg)
                
                print(f"🔍 DEBUG - Após filtro por centro de custo '{tenant_segment_id}': {len(messages_filtradas)} mensagens")
                return messages_filtradas
            
            # Se não há filtro, retornar todas as mensagens
            print(f"🔍 DEBUG - Sem filtro de centro de custo, retornando todas as {len(messages)} mensagens")
            return messages
    except requests.exceptions.Timeout:
        print("❌ Timeout na requisição")
        return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição: {e}")
        return []
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
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
                    return pd.read_csv(uploaded_file, dtype=str)
                except Exception as e:
                    uploaded_file.seek(0)
                    raise e
    else:
        return pd.read_excel(uploaded_file, dtype=str)

def extrair_ura_da_base(df, data_ini=None, data_fim=None):
    """Extrai e conta registros com UTM source = 'URA' da base carregada, separados por status e opcionalmente filtrados por data."""
    ura_count = 0
    ura_por_status = {
        'Novo': 0,
        'FGTS': 0,
        'CLT': 0,
        'Outros': 0
    }
    ura_cpfs_por_status = {
        'Novo': set(),
        'FGTS': set(),
        'CLT': set(),
        'Outros': set()
    }
    
    # Verifica se há dados válidos na base
    if df is None or df.empty:
        return ura_count, ura_por_status, ura_cpfs_por_status
    
    # Procura por colunas que podem conter UTM source
    colunas_utm = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['utm', 'source', 'origem', 'fonte']):
            colunas_utm.append(col)
    
    # Se não encontrar colunas específicas, procura por qualquer coluna que contenha "utm"
    if not colunas_utm:
        for col in df.columns:
            if 'utm' in col.lower():
                colunas_utm.append(col)
    
    # Se ainda não encontrou, procura por colunas que contenham "source"
    if not colunas_utm:
        for col in df.columns:
            if 'source' in col.lower():
                colunas_utm.append(col)
    
    # Se não encontrou nenhuma coluna UTM, retorna zeros
    if not colunas_utm:
        return ura_count, ura_por_status, ura_cpfs_por_status
    
    # Procura por colunas que podem conter CPFs
    colunas_cpf = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['cpf', 'document', 'documento', 'cnpj']):
            colunas_cpf.append(col)
    
    # Se não encontrar colunas específicas de CPF, usa todas as colunas
    if not colunas_cpf:
        colunas_cpf = df.columns.tolist()
    
    # Procura por colunas de status
    colunas_status = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['status', 'categoria', 'tipo', 'segmento']):
            colunas_status.append(col)
    
    # Se não encontrar colunas específicas de status, procura por qualquer coluna que contenha "status"
    if not colunas_status:
        for col in df.columns:
            if 'status' in col.lower():
                colunas_status.append(col)
    
    # Procura por colunas de data
    colunas_data = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['data', 'date', 'criacao', 'created', 'timestamp']):
            colunas_data.append(col)
    
    # Conta registros com valor "URA"
    for idx, row in df.iterrows():
        # Verifica se tem UTM source = "URA"
        tem_ura = False
        for col in colunas_utm:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip().upper()
                if valor_str == "URA":
                    tem_ura = True
                    break
        
        if tem_ura:
            # Se há filtro de data, verifica se está no período
            if data_ini and data_fim and colunas_data:
                data_valida = False
                for col in colunas_data:
                    try:
                        data_str = str(row[col])
                        if pd.notna(data_str) and data_str.strip():
                            # Tenta diferentes formatos de data
                            data_criacao = None
                            
                            # Formato: DD/MM/YYYY HH:MM
                            if len(data_str) >= 16 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M')
                            # Formato: DD/MM/YYYY
                            elif len(data_str) == 10 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str, '%d/%m/%Y')
                            # Formato: YYYY-MM-DD HH:MM:SS
                            elif len(data_str) >= 19:
                                data_criacao = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
                            # Formato: YYYY-MM-DD
                            elif len(data_str) == 10:
                                data_criacao = datetime.strptime(data_str, '%Y-%m-%d')
                            
                            if data_criacao:
                                data_ini_dt = datetime.combine(data_ini, datetime.min.time())
                                data_fim_dt = datetime.combine(data_fim, datetime.max.time())
                                if data_ini_dt <= data_criacao <= data_fim_dt:
                                    data_valida = True
                                    break
                    except (ValueError, TypeError):
                        continue
                
                # Se não há filtro de data ou se a data está no período, conta o registro
                if data_valida:
                    ura_count += 1
                    # Extrai CPF do registro
                    cpf_encontrado = None
                    for col in colunas_cpf:
                        valor_cpf = row[col] if col in row else None
                        if valor_cpf is not None:
                            valor_cpf_str = str(valor_cpf).strip()
                            # Usar a nova função de limpeza de CPF
                            cpf_limpo = limpar_cpf(valor_cpf_str)
                            if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                        cpf_encontrado = cpf_limpo
                                        break
                    
                    # Categoriza por status
                    status_encontrado = False
                    for col in colunas_status:
                        valor_status = row[col] if col in row else None
                        if valor_status is not None:
                            valor_status_str = str(valor_status).strip().upper()
                            if valor_status_str.startswith('INSS'):
                                ura_por_status['Novo'] += 1
                                if cpf_encontrado:
                                    ura_cpfs_por_status['Novo'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('FGTS'):
                                ura_por_status['FGTS'] += 1
                                if cpf_encontrado:
                                    ura_cpfs_por_status['FGTS'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('CLT'):
                                ura_por_status['CLT'] += 1
                                if cpf_encontrado:
                                    ura_cpfs_por_status['CLT'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                    
                    if not status_encontrado:
                        ura_por_status['Outros'] += 1
                        if cpf_encontrado:
                            ura_cpfs_por_status['Outros'].add(cpf_encontrado)
            else:
                # Se não há filtro de data, conta todos os registros URA
                ura_count += 1
                # Extrai CPF do registro
                cpf_encontrado = None
                for col in colunas_cpf:
                    valor_cpf = row[col] if col in row else None
                    if valor_cpf is not None:
                        valor_cpf_str = str(valor_cpf).strip()
                        # Usar a nova função de limpeza de CPF
                        cpf_limpo = limpar_cpf(valor_cpf_str)
                        if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                cpf_encontrado = cpf_limpo
                                break
                
                # Categoriza por status
                status_encontrado = False
                for col in colunas_status:
                    valor_status = row[col] if col in row else None
                    if valor_status is not None:
                        valor_status_str = str(valor_status).strip().upper()
                        if valor_status_str.startswith('INSS'):
                            ura_por_status['Novo'] += 1
                            if cpf_encontrado:
                                ura_cpfs_por_status['Novo'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('FGTS'):
                            ura_por_status['FGTS'] += 1
                            if cpf_encontrado:
                                ura_cpfs_por_status['FGTS'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('CLT'):
                            ura_por_status['CLT'] += 1
                            if cpf_encontrado:
                                ura_cpfs_por_status['CLT'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                
                if not status_encontrado:
                    ura_por_status['Outros'] += 1
                    if cpf_encontrado:
                        ura_cpfs_por_status['Outros'].add(cpf_encontrado)
    
    return ura_count, ura_por_status, ura_cpfs_por_status

def filtrar_mensagens_por_data(messages, data_ini, data_fim):
    """Filtra mensagens do Kolmeya por período de data."""
    if not messages or not data_ini or not data_fim:
        return messages
    
    # CORREÇÃO: Usar timezone brasileiro para consistência
    from datetime import timezone, timedelta
    tz_brasil = timezone(timedelta(hours=-3))
    
    # Converte as datas para datetime com timezone brasileiro
    data_ini_dt = datetime.combine(data_ini, datetime.min.time()).replace(tzinfo=tz_brasil)
    data_fim_dt = datetime.combine(data_fim, datetime.max.time()).replace(tzinfo=tz_brasil)
    
    print(f"🔍 DEBUG - Filtro por data (fuso BR):")
    print(f"   📅 Data inicial: {data_ini} -> {data_ini_dt}")
    print(f"   📅 Data final: {data_fim} -> {data_fim_dt}")
    print(f"   📊 Mensagens antes do filtro: {len(messages)}")
    
    mensagens_filtradas = []
    mensagens_processadas = 0
    
    for msg in messages:
        if isinstance(msg, dict):
            # Campo 'enviada_em' da nova API (formato: dd/mm/yyyy hh:mm)
            if 'enviada_em' in msg and msg['enviada_em']:
                try:
                    data_str = str(msg['enviada_em'])
                    # Formato: DD/MM/YYYY HH:MM
                    if len(data_str) >= 16 and '/' in data_str:
                        # CORREÇÃO: Assumir que a data da API está no fuso brasileiro
                        data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M').replace(tzinfo=tz_brasil)
                        mensagens_processadas += 1
                        
                        # Se está no período, inclui a mensagem
                        if data_ini_dt <= data_criacao <= data_fim_dt:
                            mensagens_filtradas.append(msg)
                            if mensagens_processadas <= 5:  # Mostrar apenas as primeiras 5 para debug
                                print(f"   ✅ Mensagem incluída: {data_str} (criada em {data_criacao})")
                        else:
                            if mensagens_processadas <= 5:  # Mostrar apenas as primeiras 5 para debug
                                print(f"   ❌ Mensagem fora do período: {data_str} (criada em {data_criacao})")
                                print(f"      Comparação: {data_ini_dt} <= {data_criacao} <= {data_fim_dt}")
                except (ValueError, TypeError) as e:
                    print(f"   ⚠️ Erro ao processar data '{data_str}': {e}")
                    continue
    
    print(f"   📊 Mensagens processadas: {mensagens_processadas}")
    print(f"   📊 Mensagens após filtro: {len(mensagens_filtradas)}")
    return mensagens_filtradas

def consultar_facta_por_cpf(cpf, token=None, data_ini=None, data_fim=None):
    """Consulta o endpoint da Facta para um CPF específico."""
    if token is None:
        token = get_facta_token()
    
    if not token:
        print("Token da Facta não encontrado")
        return None
    
    # URL da API da Facta (produção)
    url = "https://webservice.facta.com.br/proposta/andamento-propostas"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Parâmetros da consulta
    params = {
        "cpf": cpf,
        "convenio": 3,  # FACTA FINANCEIRA
        "quantidade": 5000,  # Máximo de registros por página
        "pagina": 1
    }
    
    # Adicionar filtros de data se fornecidos
    if data_ini:
        params["data_ini"] = data_ini.strftime('%d/%m/%Y')
    if data_fim:
        params["data_fim"] = data_fim.strftime('%d/%m/%Y')
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)  # Reduzido timeout
        
        if resp.status_code == 200:
            data = resp.json()
            if not data.get("erro", True):
                propostas = data.get("propostas", [])
                return propostas
            else:
                return []
        else:
            return []
            
    except Exception as e:
        return []

def consultar_facta_multiplos_cpfs(cpfs, token=None, max_workers=5, data_ini=None, data_fim=None):
    """Consulta o endpoint da Facta para múltiplos CPFs usando threads."""
    if not cpfs:
        return {}
    
    resultados = {}
    
    def consultar_cpf(cpf):
        try:
            propostas = consultar_facta_por_cpf(cpf, token, data_ini, data_fim)
            return cpf, propostas
        except Exception as e:
            print(f"Erro ao consultar CPF {cpf}: {e}")
            return cpf, []
    
    # Usar ThreadPoolExecutor para consultas paralelas
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as consultas
        future_to_cpf = {executor.submit(consultar_cpf, cpf): cpf for cpf in cpfs}
        
        # Coletar resultados
        for future in as_completed(future_to_cpf):
            cpf = future_to_cpf[future]
            try:
                cpf_result, propostas = future.result()
                resultados[cpf_result] = propostas
            except Exception as e:
                print(f"Erro ao processar resultado para CPF {cpf}: {e}")
                resultados[cpf] = []
    
    return resultados

def analisar_propostas_facta(propostas_dict):
    """Analisa as propostas da Facta e retorna estatísticas."""
    if not propostas_dict:
        return {
            'total_cpfs_consultados': 0,
            'total_propostas': 0,
            'cpfs_com_propostas': 0,
            'cpfs_sem_propostas': 0,
            'propostas_por_status': {},
            'valor_total_propostas': 0.0,
            'valor_medio_proposta': 0.0,
            'propostas_por_produto': {},
            'propostas_por_averbador': {},
            'propostas_por_corretor': {},
            'propostas_por_tipo_operacao': {},
            'taxa_conversao': 0.0
        }
    
    total_cpfs = len(propostas_dict)
    total_propostas = 0
    cpfs_com_propostas = 0
    cpfs_sem_propostas = 0
    propostas_por_status = {}
    propostas_por_produto = {}
    propostas_por_averbador = {}
    propostas_por_corretor = {}
    propostas_por_tipo_operacao = {}
    valor_total = 0.0
    
    for cpf, propostas in propostas_dict.items():
        # Filtrar apenas propostas com status "16 - CONTRATO PAGO"
        propostas_contrato_pago = []
        for proposta in propostas:
            status = proposta.get('status_proposta', '')
            if status == '16 - CONTRATO PAGO':
                propostas_contrato_pago.append(proposta)
        
        if propostas_contrato_pago:
            cpfs_com_propostas += 1
            total_propostas += len(propostas_contrato_pago)
            
            for proposta in propostas_contrato_pago:
                # Contar por status
                status = proposta.get('status_proposta', 'Sem Status')
                propostas_por_status[status] = propostas_por_status.get(status, 0) + 1
                
                # Contar por produto
                produto = proposta.get('produto', 'Sem Produto')
                propostas_por_produto[produto] = propostas_por_produto.get(produto, 0) + 1
                
                # Contar por averbador
                averbador = proposta.get('averbador', 'Sem Averbador')
                propostas_por_averbador[averbador] = propostas_por_averbador.get(averbador, 0) + 1
                
                # Contar por corretor
                corretor = proposta.get('corretor', 'Sem Corretor')
                propostas_por_corretor[corretor] = propostas_por_corretor.get(corretor, 0) + 1
                
                # Contar por tipo de operação
                tipo_operacao = proposta.get('tipo_operacao', 'Sem Tipo')
                propostas_por_tipo_operacao[tipo_operacao] = propostas_por_tipo_operacao.get(tipo_operacao, 0) + 1
                
                # Somar valores
                valor_bruto = float(proposta.get('valor_bruto', 0))
                valor_total += valor_bruto
        else:
            cpfs_sem_propostas += 1
    
    print(f"🔍 DEBUG - Análise Facta (apenas CONTRATO PAGO):")
    print(f"   📊 Total CPFs consultados: {total_cpfs}")
    print(f"   ✅ CPFs com contratos pagos: {cpfs_com_propostas}")
    print(f"   ❌ CPFs sem contratos pagos: {cpfs_sem_propostas}")
    print(f"   💰 Total de contratos pagos: {total_propostas}")
    print(f"   💰 Valor total contratos pagos: R$ {valor_total:,.2f}")
    
    return {
        'total_cpfs_consultados': total_cpfs,
        'total_propostas': total_propostas,
        'cpfs_com_propostas': cpfs_com_propostas,
        'cpfs_sem_propostas': cpfs_sem_propostas,
        'propostas_por_status': propostas_por_status,
        'valor_total_propostas': valor_total,
        'valor_medio_proposta': valor_total / total_propostas if total_propostas > 0 else 0.0,
        'propostas_por_produto': propostas_por_produto,
        'propostas_por_averbador': propostas_por_averbador,
        'propostas_por_corretor': propostas_por_corretor,
        'propostas_por_tipo_operacao': propostas_por_tipo_operacao,
        'taxa_conversao': (cpfs_com_propostas / total_cpfs * 100) if total_cpfs > 0 else 0.0
    }

def obter_cpfs_fgts_4net_kolmeya(uploaded_file, data_ini, data_fim, messages):
    """Obtém CPFs de FGTS tanto do 4NET quanto do Kolmeya."""
    cpfs_fgts = set()
    
    # CPFs do 4NET (URA)
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            ura_count, ura_por_status, ura_cpfs_por_status = extrair_ura_da_base(df_base, data_ini, data_fim)
            
            # Adicionar CPFs de FGTS do 4NET
            cpfs_fgts.update(ura_cpfs_por_status.get('FGTS', set()))
            print(f"CPFs FGTS encontrados no 4NET: {len(ura_cpfs_por_status.get('FGTS', set()))}")
        except Exception as e:
            print(f"Erro ao extrair CPFs FGTS do 4NET: {e}")
    
    # CPFs do Kolmeya
    if messages:
        # Filtrar mensagens de FGTS
        mensagens_fgts = [msg for msg in messages if isinstance(msg, dict) and 
                         msg.get('tenant_segment_id') == 'FGTS']
        
        # Extrair CPFs das mensagens de FGTS
        cpfs_kolmeya_fgts = extrair_cpfs_kolmeya(mensagens_fgts)
        cpfs_fgts.update(cpfs_kolmeya_fgts)
        print(f"CPFs FGTS encontrados no Kolmeya: {len(cpfs_kolmeya_fgts)}")
    
    return cpfs_fgts

def extrair_whatsapp_da_base(df, data_ini=None, data_fim=None):
    """Extrai e conta registros com UTM source = 'WHATSAPP_MKT' da base carregada, separados por status e opcionalmente filtrados por data."""
    whatsapp_count = 0
    whatsapp_por_status = {
        'Novo': 0,
        'FGTS': 0,
        'CLT': 0,
        'Outros': 0
    }
    whatsapp_cpfs_por_status = {
        'Novo': set(),
        'FGTS': set(),
        'CLT': set(),
        'Outros': set()
    }
    
    # Verifica se há dados válidos na base
    if df is None or df.empty:
        return whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status
    
    # Procura por colunas que podem conter UTM source
    colunas_utm = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['utm', 'source', 'origem', 'fonte']):
            colunas_utm.append(col)
    
    # Se não encontrar colunas específicas, procura por qualquer coluna que contenha "utm"
    if not colunas_utm:
        for col in df.columns:
            if 'utm' in col.lower():
                colunas_utm.append(col)
    
    # Se ainda não encontrou, procura por colunas que contenham "source"
    if not colunas_utm:
        for col in df.columns:
            if 'source' in col.lower():
                colunas_utm.append(col)
    
    # Se não encontrou nenhuma coluna UTM, retorna zeros
    if not colunas_utm:
        return whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status
    
    # Procura por colunas que podem conter CPFs
    colunas_cpf = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['cpf', 'document', 'documento', 'cnpj']):
            colunas_cpf.append(col)
    
    # Se não encontrar colunas específicas de CPF, usa todas as colunas
    if not colunas_cpf:
        colunas_cpf = df.columns.tolist()
    
    # Procura por colunas de status
    colunas_status = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['status', 'categoria', 'tipo', 'segmento']):
            colunas_status.append(col)
    
    # Se não encontrar colunas específicas de status, procura por qualquer coluna que contenha "status"
    if not colunas_status:
        for col in df.columns:
            if 'status' in col.lower():
                colunas_status.append(col)
    
    # Procura por colunas de data
    colunas_data = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['data', 'date', 'criacao', 'created', 'timestamp']):
            colunas_data.append(col)
    
    # Conta registros com valor "WHATSAPP_MKT"
    for idx, row in df.iterrows():
        # Verifica se tem UTM source = "WHATSAPP_MKT"
        tem_whatsapp = False
        for col in colunas_utm:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip().upper()
                if valor_str == "WHATSAPP_MKT":
                    tem_whatsapp = True
                    break
        
        if tem_whatsapp:
            # Se há filtro de data, verifica se está no período
            if data_ini and data_fim and colunas_data:
                data_valida = False
                for col in colunas_data:
                    try:
                        data_str = str(row[col])
                        if pd.notna(data_str) and data_str.strip():
                            # Tenta diferentes formatos de data
                            data_criacao = None
                            
                            # Formato: DD/MM/YYYY HH:MM
                            if len(data_str) >= 16 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M')
                            # Formato: DD/MM/YYYY
                            elif len(data_str) == 10 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str, '%d/%m/%Y')
                            # Formato: YYYY-MM-DD HH:MM:SS
                            elif len(data_str) >= 19:
                                data_criacao = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
                            # Formato: YYYY-MM-DD
                            elif len(data_str) == 10:
                                data_criacao = datetime.strptime(data_str, '%Y-%m-%d')
                            
                            if data_criacao:
                                data_ini_dt = datetime.combine(data_ini, datetime.min.time())
                                data_fim_dt = datetime.combine(data_fim, datetime.max.time())
                                if data_ini_dt <= data_criacao <= data_fim_dt:
                                    data_valida = True
                                    break
                    except (ValueError, TypeError):
                        continue
                
                # Se não há filtro de data ou se a data está no período, conta o registro
                if data_valida:
                    whatsapp_count += 1
                    # Extrai CPF do registro
                    cpf_encontrado = None
                    for col in colunas_cpf:
                        valor_cpf = row[col] if col in row else None
                        if valor_cpf is not None:
                            valor_cpf_str = str(valor_cpf).strip()
                            # Usar a nova função de limpeza de CPF
                            cpf_limpo = limpar_cpf(valor_cpf_str)
                            if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                        cpf_encontrado = cpf_limpo
                                        break
                    
                    # Categoriza por status
                    status_encontrado = False
                    for col in colunas_status:
                        valor_status = row[col] if col in row else None
                        if valor_status is not None:
                            valor_status_str = str(valor_status).strip().upper()
                            if valor_status_str.startswith('INSS'):
                                whatsapp_por_status['Novo'] += 1
                                if cpf_encontrado:
                                    whatsapp_cpfs_por_status['Novo'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('FGTS'):
                                whatsapp_por_status['FGTS'] += 1
                                if cpf_encontrado:
                                    whatsapp_cpfs_por_status['FGTS'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('CLT'):
                                whatsapp_por_status['CLT'] += 1
                                if cpf_encontrado:
                                    whatsapp_cpfs_por_status['CLT'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                    
                    if not status_encontrado:
                        whatsapp_por_status['Outros'] += 1
                        if cpf_encontrado:
                            whatsapp_cpfs_por_status['Outros'].add(cpf_encontrado)
            else:
                # Se não há filtro de data, conta todos os registros WHATSAPP_MKT
                whatsapp_count += 1
                # Extrai CPF do registro
                cpf_encontrado = None
                for col in colunas_cpf:
                    valor_cpf = row[col] if col in row else None
                    if valor_cpf is not None:
                        valor_cpf_str = str(valor_cpf).strip()
                        # Usar a nova função de limpeza de CPF
                        cpf_limpo = limpar_cpf(valor_cpf_str)
                        if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                cpf_encontrado = cpf_limpo
                                break
                
                # Categoriza por status
                status_encontrado = False
                for col in colunas_status:
                    valor_status = row[col] if col in row else None
                    if valor_status is not None:
                        valor_status_str = str(valor_status).strip().upper()
                        if valor_status_str.startswith('INSS'):
                            whatsapp_por_status['Novo'] += 1
                            if cpf_encontrado:
                                whatsapp_cpfs_por_status['Novo'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('FGTS'):
                            whatsapp_por_status['FGTS'] += 1
                            if cpf_encontrado:
                                whatsapp_cpfs_por_status['FGTS'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('CLT'):
                            whatsapp_por_status['CLT'] += 1
                            if cpf_encontrado:
                                whatsapp_cpfs_por_status['CLT'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                
                if not status_encontrado:
                    whatsapp_por_status['Outros'] += 1
                    if cpf_encontrado:
                        whatsapp_cpfs_por_status['Outros'].add(cpf_encontrado)
    
    return whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status

def extrair_ad_da_base(df, data_ini=None, data_fim=None):
    """Extrai e conta registros com UTM source = 'ad' da base carregada, separados por status e opcionalmente filtrados por data."""
    ad_count = 0
    ad_por_status = {
        'Novo': 0,
        'FGTS': 0,
        'CLT': 0,
        'Outros': 0
    }
    ad_cpfs_por_status = {
        'Novo': set(),
        'FGTS': set(),
        'CLT': set(),
        'Outros': set()
    }
    
    # Verifica se há dados válidos na base
    if df is None or df.empty:
        return ad_count, ad_por_status, ad_cpfs_por_status
    
    # Procura por colunas que podem conter UTM source
    colunas_utm = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['utm', 'source', 'origem', 'fonte']):
            colunas_utm.append(col)
    
    # Se não encontrar colunas específicas, procura por qualquer coluna que contenha "utm"
    if not colunas_utm:
        for col in df.columns:
            if 'utm' in col.lower():
                colunas_utm.append(col)
    
    # Se ainda não encontrou, procura por colunas que contenham "source"
    if not colunas_utm:
        for col in df.columns:
            if 'source' in col.lower():
                colunas_utm.append(col)
    
    # Se não encontrou nenhuma coluna UTM, retorna zeros
    if not colunas_utm:
        return ad_count, ad_por_status, ad_cpfs_por_status
    
    # Procura por colunas que podem conter CPFs
    colunas_cpf = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['cpf', 'document', 'documento', 'cnpj']):
            colunas_cpf.append(col)
    
    # Se não encontrar colunas específicas de CPF, usa todas as colunas
    if not colunas_cpf:
        colunas_cpf = df.columns.tolist()
    
    # Procura por colunas de status
    colunas_status = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['status', 'categoria', 'tipo', 'segmento']):
            colunas_status.append(col)
    
    # Se não encontrar colunas específicas de status, procura por qualquer coluna que contenha "status"
    if not colunas_status:
        for col in df.columns:
            if 'status' in col.lower():
                colunas_status.append(col)
    
    # Procura por colunas de data
    colunas_data = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['data', 'date', 'criacao', 'created', 'timestamp']):
            colunas_data.append(col)
    
    # Conta registros com valor "ad"
    for idx, row in df.iterrows():
        # Verifica se tem UTM source = "ad"
        tem_ad = False
        for col in colunas_utm:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip().lower()
                if valor_str == "ad":
                    tem_ad = True
                    break
        
        if tem_ad:
            # Se há filtro de data, verifica se está no período
            if data_ini and data_fim and colunas_data:
                data_valida = False
                for col in colunas_data:
                    try:
                        data_str = str(row[col])
                        if pd.notna(data_str) and data_str.strip():
                            # Tenta diferentes formatos de data
                            data_criacao = None
                            
                            # Formato: DD/MM/YYYY HH:MM
                            if len(data_str) >= 16 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str[:16], '%d/%m/%Y %H:%M')
                            # Formato: DD/MM/YYYY
                            elif len(data_str) == 10 and '/' in data_str:
                                data_criacao = datetime.strptime(data_str, '%d/%m/%Y')
                            # Formato: YYYY-MM-DD HH:MM:SS
                            elif len(data_str) >= 19:
                                data_criacao = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
                            # Formato: YYYY-MM-DD
                            elif len(data_str) == 10:
                                data_criacao = datetime.strptime(data_str, '%Y-%m-%d')
                            
                            if data_criacao:
                                data_ini_dt = datetime.combine(data_ini, datetime.min.time())
                                data_fim_dt = datetime.combine(data_fim, datetime.max.time())
                                if data_ini_dt <= data_criacao <= data_fim_dt:
                                    data_valida = True
                                    break
                    except (ValueError, TypeError):
                        continue
                
                # Se não há filtro de data ou se a data está no período, conta o registro
                if data_valida:
                    ad_count += 1
                    # Extrai CPF do registro
                    cpf_encontrado = None
                    for col in colunas_cpf:
                        valor_cpf = row[col] if col in row else None
                        if valor_cpf is not None:
                            valor_cpf_str = str(valor_cpf).strip()
                            # Usar a nova função de limpeza de CPF
                            cpf_limpo = limpar_cpf(valor_cpf_str)
                            if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                        cpf_encontrado = cpf_limpo
                                        break
                    
                    # Categoriza por status
                    status_encontrado = False
                    for col in colunas_status:
                        valor_status = row[col] if col in row else None
                        if valor_status is not None:
                            valor_status_str = str(valor_status).strip().upper()
                            if valor_status_str.startswith('INSS'):
                                ad_por_status['Novo'] += 1
                                if cpf_encontrado:
                                    ad_cpfs_por_status['Novo'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('FGTS'):
                                ad_por_status['FGTS'] += 1
                                if cpf_encontrado:
                                    ad_cpfs_por_status['FGTS'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                            elif valor_status_str.startswith('CLT'):
                                ad_por_status['CLT'] += 1
                                if cpf_encontrado:
                                    ad_cpfs_por_status['CLT'].add(cpf_encontrado)
                                status_encontrado = True
                                break
                    
                    if not status_encontrado:
                        ad_por_status['Outros'] += 1
                        if cpf_encontrado:
                            ad_cpfs_por_status['Outros'].add(cpf_encontrado)
            else:
                # Se não há filtro de data, conta todos os registros "ad"
                ad_count += 1
                # Extrai CPF do registro
                cpf_encontrado = None
                for col in colunas_cpf:
                    valor_cpf = row[col] if col in row else None
                    if valor_cpf is not None:
                        valor_cpf_str = str(valor_cpf).strip()
                        # Usar a nova função de limpeza de CPF
                        cpf_limpo = limpar_cpf(valor_cpf_str)
                        if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                                cpf_encontrado = cpf_limpo
                                break
                
                # Categoriza por status
                status_encontrado = False
                for col in colunas_status:
                    valor_status = row[col] if col in row else None
                    if valor_status is not None:
                        valor_status_str = str(valor_status).strip().upper()
                        if valor_status_str.startswith('INSS'):
                            ad_por_status['Novo'] += 1
                            if cpf_encontrado:
                                ad_cpfs_por_status['Novo'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('FGTS'):
                            ad_por_status['FGTS'] += 1
                            if cpf_encontrado:
                                ad_cpfs_por_status['FGTS'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                        elif valor_status_str.startswith('CLT'):
                            ad_por_status['CLT'] += 1
                            if cpf_encontrado:
                                ad_cpfs_por_status['CLT'].add(cpf_encontrado)
                            status_encontrado = True
                            break
                
                if not status_encontrado:
                    ad_por_status['Outros'] += 1
                    if cpf_encontrado:
                        ad_cpfs_por_status['Outros'].add(cpf_encontrado)
    
    return ad_count, ad_por_status, ad_cpfs_por_status

def main():
    st.set_page_config(page_title="Dashboard SMS", layout="centered")
    
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=2 * 60 * 1000, key="datarefresh")
    
    # Adicionar teste de ambiente na sidebar
    test_environment_status()
    
    # IMPORTANTE: Separação clara dos dados por painel:
    # - PAINEL KOLMEYA: Dados da API do Kolmeya (SMS enviados)
    # - PAINEL 4NET: Dados da URA (UTM source = "URA") - SEMPRE separado do Kolmeya
    # - PAINEL WHATSAPP: Dados do WhatsApp (UTM source = "WHATSAPP_MKT")
    # - PAINEL AD: Dados de anúncios (UTM source = "ad")
    
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # Campos de período
    col_data_ini, col_data_fim = st.columns(2)
    with col_data_ini:
        data_ini = st.date_input("Data inicial", value=datetime.now().date() - timedelta(days=6), key="data_ini_topo")
    with col_data_fim:
        data_fim = st.date_input("Data final", value=datetime.now().date(), key="data_fim_topo")

    # Filtro de centro de custo
    centro_custo_opcoes = {
        "TODOS": None,
        "Novo": "8105",  # ID do centro de custo NOVO no Kolmeya
        "Crédito CLT": "8208",  # ID do centro de custo CRÉDITO CLT no Kolmeya
        "FGTS": "8103"  # ID do centro de custo FGTS no Kolmeya
    }
    
    centro_custo_selecionado = st.selectbox(
        "Centro de Custo",
        options=list(centro_custo_opcoes.keys()),
        index=0,  # "TODOS" será a primeira opção
        key="centro_custo_filtro"
    )
    centro_custo_valor = centro_custo_opcoes[centro_custo_selecionado]

    # Saldo Kolmeya
    col_saldo, col_vazio = st.columns([0.9, 5.1])
    
    with col_saldo:
        saldo_kolmeya = obter_saldo_kolmeya()
        st.markdown(
            f"""
            <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid rgba(162, 89, 255, 0.5); border-radius: 16px; padding: 24px 32px; color: #fff; min-width: 320px; min-height: 90px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); margin-bottom: 24px; display: flex; flex-direction: column; align-items: center;'>
                <div style='font-size: 1.3em; color: #e0d7f7; font-weight: bold; margin-bottom: 8px;'>Saldo Atual Kolmeya</div>
                <div style='font-size: 2.5em; font-weight: bold; color: #fff;'>
                    {formatar_real(saldo_kolmeya)}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    # Valores zerados para produção e vendas
    st.session_state["producao_facta_ura"] = 0.0
    st.session_state["total_vendas_facta_ura"] = 0
    st.session_state["producao_facta_kolmeya"] = 0.0
    st.session_state["total_vendas_facta_kolmeya"] = 0
    st.session_state["producao_facta_total"] = 0.0
    st.session_state["total_vendas_facta_total"] = 0
    
    # Valores zerados para WhatsApp
    st.session_state["producao_facta_whatsapp"] = 0.0
    st.session_state["total_vendas_facta_whatsapp"] = 0
    
    # Valores zerados para AD
    st.session_state["producao_facta_ad"] = 0.0
    st.session_state["total_vendas_facta_ad"] = 0
    
    # Upload de base local (movido para antes do cálculo para que os dados estejam disponíveis)
    uploaded_file = st.file_uploader("Faça upload da base de CPFs/Telefones (Excel ou CSV)", type=["csv", "xlsx"])
    
    # Inicializar variáveis para contagem de URA
    ura_count = 0
    ura_por_status = {
        'Novo': 0,
        'FGTS': 0,
        'CLT': 0,
        'Outros': 0
    }
    ura_cpfs_por_status = {
        'Novo': set(),
        'FGTS': set(),
        'CLT': set(),
        'Outros': set()
    }
    
    # Obter dados do Kolmeya via API ANTES de calcular leads
    print(f"🔍 Consultando API Kolmeya:")
    print(f"   📅 Período: {data_ini} a {data_fim}")
    print(f"   🏢 Centro de custo: {centro_custo_selecionado}")
    print(f"   🕐 Data atual: {datetime.now().date()}")
    print(f"   🔍 É dia atual? {data_fim == datetime.now().date()}")
    
    messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, centro_custo_valor)
    
    # Filtrar mensagens por data após receber da API
    if messages:
        print(f"📊 Mensagens recebidas da API: {len(messages)}")
        messages = filtrar_mensagens_por_data(messages, data_ini, data_fim)
        print(f"📅 Após filtro por data: {len(messages)} mensagens")
    else:
        print(f"⚠️ Nenhuma mensagem recebida da API")
    
    print(f"📊 Resultado final: {len(messages) if messages else 0} SMS, {total_acessos} acessos")
    
    # CALCULAR LEADS GERADOS ANTES DA RENDERIZAÇÃO DO HTML
    total_leads_gerados = 0
    telefones_base = 0
    
    # Se há base carregada, fazer processamento
    if uploaded_file is not None:
        try:
            # Extrair telefones da base
            telefones_base_temp = extrair_telefones_da_base(uploaded_file, data_ini, data_fim)
            
            # Para o painel 4NET, usar APENAS dados da URA (UTM source = "URA")
            if centro_custo_selecionado == "Novo":
                total_leads_gerados = ura_por_status.get('Novo', 0)
            elif centro_custo_selecionado == "FGTS":
                total_leads_gerados = ura_por_status.get('FGTS', 0)
            elif centro_custo_selecionado == "Crédito CLT":
                total_leads_gerados = ura_por_status.get('CLT', 0)
            else:
                total_leads_gerados = ura_count
            telefones_base = total_leads_gerados
            
            print(f"🔍 Leads Gerados - Base: {len(telefones_base_temp)}, URA: {total_leads_gerados}")
            
        except Exception as e:
            print(f"Erro ao calcular telefones coincidentes: {e}")
            # Fallback para dados da URA (painel 4NET)
            if centro_custo_selecionado == "Novo":
                total_leads_gerados = ura_por_status.get('Novo', 0)
            elif centro_custo_selecionado == "FGTS":
                total_leads_gerados = ura_por_status.get('FGTS', 0)
            elif centro_custo_selecionado == "Crédito CLT":
                total_leads_gerados = ura_por_status.get('CLT', 0)
            else:
                total_leads_gerados = ura_count
            telefones_base = total_leads_gerados
    else:
        # Se não há base ou mensagens, usar apenas dados da URA (painel 4NET)
        if centro_custo_selecionado == "Novo":
            total_leads_gerados = ura_por_status.get('Novo', 0)
        elif centro_custo_selecionado == "FGTS":
            total_leads_gerados = ura_por_status.get('FGTS', 0)
        elif centro_custo_selecionado == "Crédito CLT":
            total_leads_gerados = ura_por_status.get('CLT', 0)
        else:
            total_leads_gerados = ura_count
        telefones_base = total_leads_gerados
    



    
    # Inicializar variáveis para contagem de URA
    ura_count = 0
    ura_por_status = {
        'Novo': 0,
        'FGTS': 0,
        'CLT': 0,
        'Outros': 0
    }
    ura_cpfs_por_status = {
        'Novo': set(),
        'FGTS': set(),
        'CLT': set(),
        'Outros': set()
    }
    
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            # Extrair contagem de URA da base com filtro de data e separação por status
            ura_count, ura_por_status, ura_cpfs_por_status = extrair_ura_da_base(df_base, data_ini, data_fim)
            
            # CONSULTA AUTOMÁTICA NA FACTA
            # Obter CPFs para consulta na Facta baseado no centro de custo selecionado
            cpfs_para_consulta = set()
            
            if centro_custo_selecionado == "Novo":
                cpfs_para_consulta = ura_cpfs_por_status.get('Novo', set())
            elif centro_custo_selecionado == "FGTS":
                cpfs_para_consulta = ura_cpfs_por_status.get('FGTS', set())
            elif centro_custo_selecionado == "Crédito CLT":
                cpfs_para_consulta = ura_cpfs_por_status.get('CLT', set())
            else:
                # Se "TODOS", usar todos os CPFs
                for cpfs_status in ura_cpfs_por_status.values():
                    cpfs_para_consulta.update(cpfs_status)
            
            if cpfs_para_consulta:
                # Consultar Facta para os CPFs encontrados
                try:
                    propostas_facta = consultar_facta_multiplos_cpfs(
                        list(cpfs_para_consulta), 
                        token=None, 
                        max_workers=3, 
                        data_ini=data_ini, 
                        data_fim=data_fim
                    )
                    
                    # Analisar resultados da Facta
                    if propostas_facta:
                        analise_facta = analisar_propostas_facta(propostas_facta)
                        
                        # Atualizar métricas com dados da Facta (URA)
                        st.session_state["producao_facta_ura"] = analise_facta['valor_total_propostas']
                        st.session_state["total_vendas_facta_ura"] = analise_facta['total_propostas']
                    else:
                        st.session_state["producao_facta_ura"] = 0.0
                        st.session_state["total_vendas_facta_ura"] = 0
                        
                except Exception:
                    st.session_state["producao_facta_ura"] = 0.0
                    st.session_state["total_vendas_facta_ura"] = 0
            else:
                st.session_state["producao_facta_ura"] = 0.0
                st.session_state["total_vendas_facta_ura"] = 0
            
            # Processar dados do WhatsApp e AD silenciosamente
            try:
                whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status = extrair_whatsapp_da_base(df_base, data_ini, data_fim)
                ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
                
                # Processar WhatsApp na Facta
                if whatsapp_count > 0:
                    cpfs_whatsapp_para_consulta = set()
                    if centro_custo_selecionado == "Novo":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('Novo', set())
                    elif centro_custo_selecionado == "FGTS":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('FGTS', set())
                    elif centro_custo_selecionado == "Crédito CLT":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('CLT', set())
                    else:
                        for cpfs_status in whatsapp_cpfs_por_status.values():
                            cpfs_whatsapp_para_consulta.update(cpfs_status)
                    
                    if cpfs_whatsapp_para_consulta:
                        try:
                            propostas_facta_whatsapp = consultar_facta_multiplos_cpfs(
                                list(cpfs_whatsapp_para_consulta), 
                                token=None, 
                                max_workers=3, 
                                data_ini=data_ini, 
                                data_fim=data_fim
                            )
                            
                            if propostas_facta_whatsapp:
                                analise_facta_whatsapp = analisar_propostas_facta(propostas_facta_whatsapp)
                                st.session_state["producao_facta_whatsapp"] = analise_facta_whatsapp['valor_total_propostas']
                                st.session_state["total_vendas_facta_whatsapp"] = analise_facta_whatsapp['total_propostas']
                            else:
                                st.session_state["producao_facta_whatsapp"] = 0.0
                                st.session_state["total_vendas_facta_whatsapp"] = 0
                        except Exception:
                            st.session_state["producao_facta_whatsapp"] = 0.0
                            st.session_state["total_vendas_facta_whatsapp"] = 0
                    else:
                        st.session_state["producao_facta_whatsapp"] = 0.0
                        st.session_state["total_vendas_facta_whatsapp"] = 0
                else:
                    st.session_state["producao_facta_whatsapp"] = 0.0
                    st.session_state["total_vendas_facta_whatsapp"] = 0
                
                # Processar AD na Facta
                if ad_count > 0:
                    cpfs_ad_para_consulta = set()
                    if centro_custo_selecionado == "Novo":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('Novo', set())
                    elif centro_custo_selecionado == "FGTS":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('FGTS', set())
                    elif centro_custo_selecionado == "Crédito CLT":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('CLT', set())
                    else:
                        for cpfs_status in ad_cpfs_por_status.values():
                            cpfs_ad_para_consulta.update(cpfs_status)
                    
                    if cpfs_ad_para_consulta:
                        try:
                            propostas_facta_ad = consultar_facta_multiplos_cpfs(
                                list(cpfs_ad_para_consulta), 
                                token=None, 
                                max_workers=3, 
                                data_ini=data_ini, 
                                data_fim=data_fim
                            )
                            
                            if propostas_facta_ad:
                                analise_facta_ad = analisar_propostas_facta(propostas_facta_ad)
                                st.session_state["producao_facta_ad"] = analise_facta_ad['valor_total_propostas']
                                st.session_state["total_vendas_facta_ad"] = analise_facta_ad['total_propostas']
                            else:
                                st.session_state["producao_facta_ad"] = 0.0
                                st.session_state["total_vendas_facta_ad"] = 0
                        except Exception:
                            st.session_state["producao_facta_ad"] = 0.0
                            st.session_state["total_vendas_facta_ad"] = 0
                    else:
                        st.session_state["producao_facta_ad"] = 0.0
                        st.session_state["total_vendas_facta_ad"] = 0
                else:
                    st.session_state["producao_facta_ad"] = 0.0
                    st.session_state["total_vendas_facta_ad"] = 0
                    
            except Exception:
                st.session_state["producao_facta_whatsapp"] = 0.0
                st.session_state["total_vendas_facta_whatsapp"] = 0
                st.session_state["producao_facta_ad"] = 0.0
                st.session_state["total_vendas_facta_ad"] = 0
                
        except Exception:
            # Em caso de erro, manter valores em zero
            ura_count = 0
            ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
            ura_cpfs_por_status = {'Novo': set(), 'FGTS': set(), 'CLT': set(), 'Outros': set()}
            st.session_state["producao_facta_ura"] = 0.0
            st.session_state["total_vendas_facta_ura"] = 0
            st.session_state["producao_facta_whatsapp"] = 0.0
            st.session_state["total_vendas_facta_whatsapp"] = 0
            st.session_state["producao_facta_ad"] = 0.0
            st.session_state["total_vendas_facta_ad"] = 0
                
            # Processar dados do WhatsApp e AD silenciosamente
            try:
                whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status = extrair_whatsapp_da_base(df_base, data_ini, data_fim)
                ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
                
                # Processar WhatsApp na Facta
                if whatsapp_count > 0:
                    cpfs_whatsapp_para_consulta = set()
                    if centro_custo_selecionado == "Novo":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('Novo', set())
                    elif centro_custo_selecionado == "FGTS":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('FGTS', set())
                    elif centro_custo_selecionado == "Crédito CLT":
                        cpfs_whatsapp_para_consulta = whatsapp_cpfs_por_status.get('CLT', set())
                    else:
                        for cpfs_status in whatsapp_cpfs_por_status.values():
                            cpfs_whatsapp_para_consulta.update(cpfs_status)
                    
                    if cpfs_whatsapp_para_consulta:
                        try:
                            propostas_facta_whatsapp = consultar_facta_multiplos_cpfs(
                                list(cpfs_whatsapp_para_consulta), 
                                token=None, 
                                max_workers=3, 
                                data_ini=data_ini, 
                                data_fim=data_fim
                            )
                            
                            if propostas_facta_whatsapp:
                                analise_facta_whatsapp = analisar_propostas_facta(propostas_facta_whatsapp)
                                st.session_state["producao_facta_whatsapp"] = analise_facta_whatsapp['valor_total_propostas']
                                st.session_state["total_vendas_facta_whatsapp"] = analise_facta_whatsapp['total_propostas']
                            else:
                                st.session_state["producao_facta_whatsapp"] = 0.0
                                st.session_state["total_vendas_facta_whatsapp"] = 0
                        except Exception:
                            st.session_state["producao_facta_whatsapp"] = 0.0
                            st.session_state["total_vendas_facta_whatsapp"] = 0
                    else:
                        st.session_state["producao_facta_whatsapp"] = 0.0
                        st.session_state["total_vendas_facta_whatsapp"] = 0
                else:
                    st.session_state["producao_facta_whatsapp"] = 0.0
                    st.session_state["total_vendas_facta_whatsapp"] = 0
                
                # Processar AD na Facta
                if ad_count > 0:
                    cpfs_ad_para_consulta = set()
                    if centro_custo_selecionado == "Novo":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('Novo', set())
                    elif centro_custo_selecionado == "FGTS":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('FGTS', set())
                    elif centro_custo_selecionado == "Crédito CLT":
                        cpfs_ad_para_consulta = ad_cpfs_por_status.get('CLT', set())
                    else:
                        for cpfs_status in ad_cpfs_por_status.values():
                            cpfs_ad_para_consulta.update(cpfs_status)
                    
                    if cpfs_ad_para_consulta:
                        try:
                            propostas_facta_ad = consultar_facta_multiplos_cpfs(
                                list(cpfs_ad_para_consulta), 
                                token=None, 
                                max_workers=3, 
                                data_ini=data_ini, 
                                data_fim=data_fim
                            )
                            
                            if propostas_facta_ad:
                                analise_facta_ad = analisar_propostas_facta(propostas_facta_ad)
                                st.session_state["producao_facta_ad"] = analise_facta_ad['valor_total_propostas']
                                st.session_state["total_vendas_facta_ad"] = analise_facta_ad['total_propostas']
                            else:
                                st.session_state["producao_facta_ad"] = 0.0
                                st.session_state["total_vendas_facta_ad"] = 0
                        except Exception:
                            st.session_state["producao_facta_ad"] = 0.0
                            st.session_state["total_vendas_facta_ad"] = 0
                    else:
                        st.session_state["producao_facta_ad"] = 0.0
                        st.session_state["total_vendas_facta_ad"] = 0
                else:
                    st.session_state["producao_facta_ad"] = 0.0
                    st.session_state["total_vendas_facta_ad"] = 0
                    
            except Exception:
                st.session_state["producao_facta_whatsapp"] = 0.0
                st.session_state["total_vendas_facta_whatsapp"] = 0
                st.session_state["producao_facta_ad"] = 0.0
                st.session_state["total_vendas_facta_ad"] = 0
        except Exception:
            # Em caso de erro, manter valores em zero
            ura_count = 0
            ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
            ura_cpfs_por_status = {'Novo': set(), 'FGTS': set(), 'CLT': set(), 'Outros': set()}
            st.session_state["producao_facta_ura"] = 0.0
            st.session_state["total_vendas_facta_ura"] = 0
            st.session_state["producao_facta_whatsapp"] = 0.0
            st.session_state["total_vendas_facta_whatsapp"] = 0
            st.session_state["producao_facta_ad"] = 0.0
            st.session_state["total_vendas_facta_ad"] = 0
    else:
        # Se não há arquivo carregado, deixar o painel 4NET vazio (sem dados URA)
        ura_count = 0
        ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
        ura_cpfs_por_status = {'Novo': set(), 'FGTS': set(), 'CLT': set(), 'Outros': set()}

    # CONSULTA ADICIONAL NA FACTA PARA FGTS (COM CPFs DO KOLMEYA)
    
    # CONSULTA ADICIONAL NA FACTA PARA FGTS (COM CPFs DO KOLMEYA)
    if centro_custo_selecionado == "FGTS" and messages:
        # Extrair CPFs do Kolmeya
        cpfs_kolmeya = extrair_cpfs_kolmeya(messages)
        
        if cpfs_kolmeya:
            # Consultar Facta para os CPFs do Kolmeya
            try:
                propostas_facta_kolmeya = consultar_facta_multiplos_cpfs(
                    list(cpfs_kolmeya), 
                    token=None, 
                    max_workers=3, 
                    data_ini=data_ini, 
                    data_fim=data_fim
                )
                
                # Analisar resultados da Facta para CPFs do Kolmeya
                if propostas_facta_kolmeya:
                    analise_facta_kolmeya = analisar_propostas_facta(propostas_facta_kolmeya)
                    
                    # Manter dados separados para os painéis
                    st.session_state["producao_facta_kolmeya"] = analise_facta_kolmeya['valor_total_propostas']
                    st.session_state["total_vendas_facta_kolmeya"] = analise_facta_kolmeya['total_propostas']
                    
                    # Calcular totais para FGTS
                    producao_total = st.session_state.get("producao_facta_ura", 0.0) + analise_facta_kolmeya['valor_total_propostas']
                    vendas_total = st.session_state.get("total_vendas_facta_ura", 0) + analise_facta_kolmeya['total_propostas']
                    
                    st.session_state["producao_facta_total"] = producao_total
                    st.session_state["total_vendas_facta_total"] = vendas_total
                    
            except Exception:
                pass
    

    



    # Layout simplificado com HTML puro - sem componentes Streamlit
    st.markdown("""
    <style>
    .dashboard-container {
        display: flex;
        gap: 20px;
        margin: 20px 0;
        flex-wrap: wrap;
    }
    .panel {
        flex: 1;
        min-width: 280px;
        background: #1e1e1e;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 20px;
        color: white;
        font-family: Arial, sans-serif;
    }
    .panel-kolmeya {
        background: linear-gradient(135deg, #2d1b69 0%, #1a103f 100%);
        border: 1px solid #a259ff;
    }
    .panel-title {
        text-align: center;
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 20px;
        color: #fff;
    }
    .metric-row {
        display: flex;
        justify-content: space-between;
        margin-bottom: 15px;
    }
    .metric-item {
        text-align: center;
        flex: 1;
    }
    .metric-label {
        font-size: 12px;
        color: #ccc;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 20px;
        font-weight: bold;
        color: #fff;
    }
    .metric-value-small {
        font-size: 16px;
        font-weight: bold;
        color: #fff;
    }
    .details-section {
        background: rgba(0,0,0,0.3);
        border-radius: 6px;
        padding: 15px;
        margin: 15px 0;
    }
    .details-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 15px;
    }
    .detail-item {
        text-align: center;
        padding: 8px;
    }
    .detail-label {
        font-size: 11px;
        color: #aaa;
        margin-bottom: 3px;
    }
    .detail-value {
        font-size: 14px;
        color: #fff;
        font-weight: bold;
    }
    .roi-section {
        text-align: center;
        margin-top: 15px;
        padding: 10px;
        background: rgba(255,255,255,0.1);
        border-radius: 6px;
    }
    .roi-label {
        font-size: 14px;
        color: #ccc;
        margin-bottom: 5px;
    }
    .roi-value {
        font-size: 24px;
        font-weight: bold;
        color: #fff;
    }
    </style>
    """, unsafe_allow_html=True)

    # Dados reais do Kolmeya via API
    total_mensagens = len(messages) if messages else 0
    mensagens_entregues = len([msg for msg in messages if msg.get('status') == 'delivered']) if messages else 0
    investimento = total_mensagens * 0.08
    
    # Debug: Verificar dados recebidos
    print(f"🔍 DEBUG - Dados Kolmeya recebidos:")
    print(f"   📊 Total de mensagens: {total_mensagens}")
    print(f"   ✅ Mensagens entregues: {mensagens_entregues}")
    if messages and len(messages) > 0:
        print(f"   📅 Primeira mensagem - enviada_em: {messages[0].get('enviada_em', 'N/A')}")
        print(f"   📅 Última mensagem - enviada_em: {messages[-1].get('enviada_em', 'N/A')}")
        print(f"   🏢 Centro de custo da primeira: {messages[0].get('centro_custo', 'N/A')}")
    print(f"   💰 Investimento calculado: R$ {investimento:.2f}")
    
    # CAMPO 1: Taxa de entrega
    taxa_entrega = (mensagens_entregues / total_mensagens * 100) if total_mensagens > 0 else 0.0
    
    # CORREÇÃO: Calcular leads gerados comparando telefones da API com telefones da base
    telefones_kolmeya = extrair_telefones_kolmeya(messages) if messages else set()
    telefones_base_kolmeya = set()
    
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            telefones_base_kolmeya = extrair_telefones_da_base(df_base, data_ini, data_fim)
            
            # Calcular telefones coincidentes (leads gerados)
            telefones_coincidentes = telefones_kolmeya & telefones_base_kolmeya
            leads_gerados_kolmeya = len(telefones_coincidentes)
            
            print(f"🔍 DEBUG - Comparação Kolmeya vs Base:")
            print(f"   📱 Telefones API Kolmeya: {len(telefones_kolmeya)}")
            print(f"   📱 Telefones Base: {len(telefones_base_kolmeya)}")
            print(f"   ✅ Telefones Coincidentes (Leads Gerados): {leads_gerados_kolmeya}")
        except Exception as e:
            print(f"❌ Erro ao comparar telefones: {e}")
            leads_gerados_kolmeya = 0
    else:
        leads_gerados_kolmeya = 0
        print(f"⚠️ Nenhuma base carregada para comparação")
    
    # Dados reais do Kolmeya - usar dados da Facta quando disponíveis
    if centro_custo_selecionado == "FGTS":
        # Para FGTS, usar dados do Kolmeya da Facta
        total_vendas = st.session_state.get("total_vendas_facta_kolmeya", 0)
        producao = st.session_state.get("producao_facta_kolmeya", 0.0)
    else:
        # Para outros centros de custo, dados virão da Facta (URA)
        total_vendas = 0  # Dados reais virão da Facta
        producao = 0.0    # Dados reais virão da Facta
    
    # Previsão de faturamento (comissão de 17.1%)
    previsao_faturamento = producao * 0.171
    
    # CAMPO 5: Ticket Médio - calculado com dados reais
    if total_vendas > 0 and producao > 0:
        ticket_medio = producao / total_vendas
    else:
        ticket_medio = 0.0
    
    # CAMPO 6: ROI - calculado com dados reais
    if producao > 0 and investimento > 0:
        roi = producao - investimento
    else:
        roi = 0.0
    
    # CAMPO 2: Interação (Disparos por Lead) - dados reais da API
    disparos_por_lead = total_acessos / total_mensagens * 100 if total_mensagens > 0 else 0


    # Dados do painel 4NET baseados APENAS nos dados da URA (UTM source = "URA")
    # Filtrar dados da URA baseado no centro de custo selecionado
    if centro_custo_selecionado == "Novo":
        total_atendidas = ura_por_status.get('Novo', 0)
        telefones_base_ura = ura_por_status.get('Novo', 0)
        ligacoes_realizadas = total_atendidas
    elif centro_custo_selecionado == "FGTS":
        total_atendidas = ura_por_status.get('FGTS', 0)
        telefones_base_ura = ura_por_status.get('FGTS', 0)
        ligacoes_realizadas = total_atendidas
    elif centro_custo_selecionado == "Crédito CLT":
        total_atendidas = ura_por_status.get('CLT', 0)
        telefones_base_ura = ura_por_status.get('CLT', 0)
        ligacoes_realizadas = total_atendidas
    else:
        # Se "TODOS", usar o total da URA
        total_atendidas = ura_count
        telefones_base_ura = ura_count
        ligacoes_realizadas = ura_count
    
    # Valores baseados em dados reais ou estimativas conservadoras
    lig_atendidas = 0.0  # Será calculado com base nos dados reais
    total_investimento = ligacoes_realizadas * 0.15  # Custo por ligação
    tempo_medio_resposta = 0.0  # Será calculado com base nos dados reais
    
    # Calcular métricas baseadas nos dados da URA (não misturar com Kolmeya)
    taxa_ativacao = (total_atendidas / telefones_base_ura * 100) if telefones_base_ura > 0 else 0.0
    
    # Dados do painel 4NET - usar APENAS dados da URA (não misturar com Kolmeya)
    # Para todos os centros de custo, usar apenas dados da URA
    total_vendas_ura = st.session_state.get("total_vendas_facta_ura", 0)
    producao_ura = st.session_state.get("producao_facta_ura", 0.0)
    
    # Calcular métricas baseadas nos dados da Facta
    if total_vendas_ura > 0 and producao_ura > 0:
        fat_med_venda = producao_ura / total_vendas_ura  # Faturamento médio por venda
        retor_estimado = producao_ura * 0.171  # Retorno estimado (comissão de 17.1%)
    else:
        fat_med_venda = 0.0
        retor_estimado = 0.0

    # ROI do painel 4NET baseado APENAS nos dados da URA
    roi_ura = producao_ura - total_investimento

    # Dados do PAINEL WHATSAPP baseados nos dados reais da base e Facta
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status = extrair_whatsapp_da_base(df_base, data_ini, data_fim)
            
            # Usar dados reais do WhatsApp
            campanhas_realizadas = whatsapp_count  # Total de mensagens WhatsApp
            camp_atendidas = (whatsapp_count / max(telefones_base, 1)) * 100 if telefones_base > 0 else 0.0  # Taxa de engajamento
            total_investimento_novo = whatsapp_count * 0.20  # Custo por mensagem WhatsApp
            tempo_medio_campanha = 2.5  # Tempo médio de resposta em horas
            total_engajados = whatsapp_count  # Total de mensagens enviadas
            
            # Usar dados reais da Facta se disponíveis, senão usar estimativas
            if st.session_state.get("producao_facta_whatsapp", 0) > 0:
                # Dados reais da Facta
                total_vendas_novo = st.session_state.get("total_vendas_facta_whatsapp", 0)
                producao_novo = st.session_state.get("producao_facta_whatsapp", 0.0)
                # Dados reais da Facta
                total_vendas_novo = st.session_state.get("total_vendas_facta_whatsapp", 0)
                producao_novo = st.session_state.get("producao_facta_whatsapp", 0.0)
            else:
                # Estimativas baseadas em conversão
                total_vendas_novo = whatsapp_count * 0.15  # Estimativa de vendas (15% de conversão)
                producao_novo = total_vendas_novo * 5000  # Produção estimada (ticket médio R$ 5.000)
            
            roi_novo = producao_novo - total_investimento_novo
        except Exception as e:
            print(f"Erro ao processar dados do WhatsApp: {e}")
            # Fallback para valores padrão
            campanhas_realizadas = 0
            camp_atendidas = 0.0
            total_investimento_novo = 0.0
            tempo_medio_campanha = 0.0
            total_engajados = 0
            total_vendas_novo = 0
            producao_novo = 0.0
            roi_novo = 0.0
    else:
        # Se não há arquivo carregado, usar valores padrão
        campanhas_realizadas = 0
        camp_atendidas = 0.0
        total_investimento_novo = 0.0
        tempo_medio_campanha = 0.0
        total_engajados = 0
        total_vendas_novo = 0
        producao_novo = 0.0
        roi_novo = 0.0

    # Dados do TERCEIRO PAINEL baseados nos dados reais da base e Facta
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
            
            # Usar dados reais do AD
            acoes_realizadas = ad_count  # Total de ações AD
            acoes_efetivas = (ad_count / max(telefones_base, 1)) * 100 if telefones_base > 0 else 0.0  # Taxa de efetividade
            total_investimento_segundo = ad_count * 0.25  # Custo por ação AD
            tempo_medio_acao = 3.0  # Tempo médio de resposta em horas
            total_efetivos = ad_count  # Total de ações realizadas
            
            # Usar dados reais da Facta se disponíveis, senão usar estimativas
            if st.session_state.get("producao_facta_ad", 0) > 0:
                # Dados reais da Facta
                total_vendas_segundo = st.session_state.get("total_vendas_facta_ad", 0)
                producao_segundo = st.session_state.get("producao_facta_ad", 0.0)
                # Dados reais da Facta
                total_vendas_segundo = st.session_state.get("total_vendas_facta_ad", 0)
                producao_segundo = st.session_state.get("producao_facta_ad", 0.0)
            else:
                # Estimativas baseadas em conversão
                total_vendas_segundo = ad_count * 0.12  # Estimativa de vendas (12% de conversão)
                producao_segundo = total_vendas_segundo * 4500  # Produção estimada (ticket médio R$ 4.500)
            
            roi_segundo = producao_segundo - total_investimento_segundo
        except Exception as e:
            print(f"Erro ao processar dados do AD: {e}")
            # Fallback para valores padrão
            acoes_realizadas = 0
            acoes_efetivas = 0.0
            total_investimento_segundo = 0.0
            tempo_medio_acao = 0.0
            total_efetivos = 0
            total_vendas_segundo = 0
            producao_segundo = 0.0
            roi_segundo = 0.0
    else:
        # Se não há arquivo carregado, usar valores padrão
        acoes_realizadas = 0
        acoes_efetivas = 0.0
        total_investimento_segundo = 0.0
        tempo_medio_acao = 0.0
        total_efetivos = 0
        total_vendas_segundo = 0
        producao_segundo = 0.0
        roi_segundo = 0.0

        # Inicializar variáveis que podem não estar definidas
    taxa_lead = getattr(locals(), 'taxa_lead', 0.0)
    ligacoes_realizadas = getattr(locals(), 'ligacoes_realizadas', 0)
    total_investimento = getattr(locals(), 'total_investimento', 0.0)
    tempo_medio_campanha = getattr(locals(), 'tempo_medio_campanha', 0.0)
    camp_atendidas = getattr(locals(), 'camp_atendidas', 0.0)
    total_investimento_novo = getattr(locals(), 'total_investimento_novo', 0.0)
    total_vendas_novo = getattr(locals(), 'total_vendas_novo', 0)
    producao_novo = getattr(locals(), 'producao_novo', 0.0)
    total_engajados = getattr(locals(), 'total_engajados', 0)
    roi_novo = getattr(locals(), 'roi_novo', 0.0)
    acoes_realizadas = getattr(locals(), 'acoes_realizadas', 0)
    acoes_efetivas = getattr(locals(), 'acoes_efetivas', 0.0)
    total_investimento_segundo = getattr(locals(), 'total_investimento_segundo', 0.0)
    tempo_medio_acao = getattr(locals(), 'tempo_medio_acao', 0.0)
    total_vendas_segundo = getattr(locals(), 'total_vendas_segundo', 0)
    producao_segundo = getattr(locals(), 'producao_segundo', 0.0)
    total_efetivos = getattr(locals(), 'total_efetivos', 0)
    roi_segundo = getattr(locals(), 'roi_segundo', 0.0)
    
    # SALVAR MÉTRICAS NO BANCO DE DADOS - SISTEMA MELHORADO
    if HAS_DATABASE:
        try:
            # Garantir que todos os valores sejam numéricos e precisos
            def safe_float(value, default=0.0):
                """Converte valor para float de forma segura."""
                try:
                    if value is None or value == '':
                        return default
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_int(value, default=0):
                """Converte valor para int de forma segura."""
                try:
                    if value is None or value == '':
                        return default
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            # Preparar dados KOLMEYA com validação precisa
            dados_kolmeya = {
                'canal': 'Kolmeya',
                'sms_enviados': safe_int(total_mensagens),
                'interacoes': safe_float(disparos_por_lead),
                'investimento': safe_float(investimento),
                'taxa_entrega': safe_float(taxa_entrega),
                'total_vendas': safe_int(total_vendas),
                'producao': safe_float(producao),
                'leads_gerados': safe_int(leads_gerados_kolmeya),
                'ticket_medio': safe_float(ticket_medio),
                'roi': safe_float(roi)
            }
            
            # Preparar dados 4NET com validação precisa
            dados_4net = {
                'canal': '4NET',
                'sms_enviados': safe_int(ura_count),  # Usar dados reais da URA
                'interacoes': safe_float(ligacoes_realizadas),
                'investimento': safe_float(total_investimento),
                'taxa_entrega': safe_float(taxa_lead),
                'total_vendas': safe_int(total_vendas_ura),
                'producao': safe_float(producao_ura),
                'leads_gerados': safe_int(telefones_base),
                'ticket_medio': safe_float(fat_med_venda),
                'roi': safe_float(roi_ura)
            }
            
            # Preparar dados WhatsApp com validação precisa
            dados_whatsapp = {
                'canal': 'WhatsApp',
                'sms_enviados': safe_int(campanhas_realizadas),
                'interacoes': safe_float(camp_atendidas),
                'investimento': safe_float(total_investimento_novo),
                'taxa_entrega': safe_float(tempo_medio_campanha),
                'total_vendas': safe_int(total_vendas_novo),
                'producao': safe_float(producao_novo),
                'leads_gerados': safe_int(total_engajados),
                'ticket_medio': safe_float(producao_novo/total_vendas_novo if total_vendas_novo > 0 else 0),
                'roi': safe_float(roi_novo)
            }
            
            # Preparar dados AD com validação precisa
            dados_ad = {
                'canal': 'AD',
                'sms_enviados': safe_int(acoes_realizadas),
                'interacoes': safe_float(acoes_efetivas),
                'investimento': safe_float(total_investimento_segundo),
                'taxa_entrega': safe_float(tempo_medio_acao),
                'total_vendas': safe_int(total_vendas_segundo),
                'producao': safe_float(producao_segundo),
                'leads_gerados': safe_int(total_efetivos),
                'ticket_medio': safe_float(producao_segundo/total_vendas_segundo if total_vendas_segundo > 0 else 0),
                'roi': safe_float(roi_segundo)
            }
            
            # Log detalhado antes de salvar
            print(f"💾 Salvando métricas precisas:")
            print(f"   Kolmeya: SMS={dados_kolmeya['sms_enviados']}, Vendas={dados_kolmeya['total_vendas']}, Produção={dados_kolmeya['producao']}")
            print(f"   4NET: SMS={dados_4net['sms_enviados']}, Vendas={dados_4net['total_vendas']}, Produção={dados_4net['producao']}")
            print(f"   WhatsApp: SMS={dados_whatsapp['sms_enviados']}, Vendas={dados_whatsapp['total_vendas']}, Produção={dados_whatsapp['producao']}")
            print(f"   AD: SMS={dados_ad['sms_enviados']}, Vendas={dados_ad['total_vendas']}, Produção={dados_ad['producao']}")
            
            # Salvar no banco de dados
            salvar_metricas_dashboard(
                dados_kolmeya, dados_4net, dados_whatsapp, dados_ad,
                centro_custo_selecionado, data_ini, data_fim
            )
            
            print(f"✅ Métricas salvas com sucesso - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar métricas no banco: {e}")
            import traceback
            traceback.print_exc()
    
    # Dashboard HTML usando st.components.html para melhor renderização
    import streamlit.components.v1 as components
    
    dashboard_html = f"""
    <style>
    .dashboard-container {{
        display: flex;
        gap: 15px;
        margin: 10px 0;
        flex-wrap: wrap;
        justify-content: space-between;
    }}
    .panel {{
        flex: 1;
        min-width: 250px;
        max-width: 300px;
        background: #1e1e1e;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 15px;
        color: white;
        font-family: Arial, sans-serif;
        margin-bottom: 10px;
    }}
    .panel-kolmeya {{
        background: linear-gradient(135deg, #2d1b69 0%, #1a103f 100%);
        border: 1px solid #a259ff;
    }}
    .panel-title {{
        text-align: center;
        font-size: 16px;
        font-weight: bold;
        margin-bottom: 15px;
        color: #fff;
    }}
    .metric-row {{
        display: flex;
        justify-content: space-between;
        margin-bottom: 12px;
    }}
    .metric-item {{
        text-align: center;
        flex: 1;
    }}
    .metric-label {{
        font-size: 11px;
        color: #ccc;
        margin-bottom: 3px;
    }}
    .metric-value {{
        font-size: 18px;
        font-weight: bold;
        color: #fff;
    }}
    .metric-value-small {{
        font-size: 14px;
        font-weight: bold;
        color: #fff;
    }}
    .details-section {{
        background: rgba(0,0,0,0.3);
        border-radius: 6px;
        padding: 12px;
        margin: 12px 0;
    }}
    .details-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
    }}
    .detail-item {{
        text-align: center;
        padding: 6px;
    }}
    .detail-label {{
        font-size: 10px;
        color: #aaa;
        margin-bottom: 2px;
    }}
    .detail-value {{
        font-size: 12px;
        color: #fff;
        font-weight: bold;
    }}
    .roi-section {{
        text-align: center;
        margin-top: 12px;
        padding: 8px;
        background: rgba(255,255,255,0.1);
        border-radius: 6px;
    }}
    .roi-label {{
        font-size: 12px;
        color: #ccc;
        margin-bottom: 3px;
    }}
    .roi-value {{
        font-size: 20px;
        font-weight: bold;
        color: #fff;
    }}
    </style>
    
    <div class="dashboard-container">
        <!-- PAINEL KOLMEYA -->
        <div class="panel panel-kolmeya">
            <div class="panel-title">KOLMEYA</div>
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">SMS Enviados</div>
                    <div class="metric-value">{total_mensagens:,}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Interação</div>
                    <div class="metric-value">{disparos_por_lead:.1f}%</div>
                </div>
            </div>
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Investimento</div>
                    <div class="metric-value-small">{formatar_real(investimento)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Taxa Entrega</div>
                    <div class="metric-value-small">{taxa_entrega:.1f}%</div>
                </div>
            </div>
            <div class="details-section">
                <div class="details-grid">
                    <div class="detail-item">
                        <div class="detail-label">Total Vendas</div>
                        <div class="detail-value">{total_vendas}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Produção</div>
                        <div class="detail-value">{formatar_real(producao)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Leads Gerados</div>
                        <div class="detail-value">{leads_gerados_kolmeya:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Ticket Médio</div>
                        <div class="detail-value">{formatar_real(ticket_medio)}</div>
                    </div>
                </div>
            </div>
            <div class="roi-section">
                <div class="roi-label">ROI</div>
                <div class="roi-value">{formatar_real(roi)}</div>
            </div>
        </div>

        <!-- PAINEL 4NET -->
        <div class="panel">
            <div class="panel-title">4NET</div>
            {f"""
                            <div class="metric-row">
                    <div class="metric-item">
                        <div class="metric-label">Ligações</div>
                        <div class="metric-value">{ligacoes_realizadas}</div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label">Atendidas</div>
                        <div class="metric-value">{total_atendidas}</div>
                    </div>
                </div>
                <div class="metric-row">
                    <div class="metric-item">
                        <div class="metric-label">Investimento</div>
                        <div class="metric-value-small">{formatar_real(total_investimento)}</div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label">Taxa Ativação</div>
                        <div class="metric-value-small">{taxa_ativacao:.1f}%</div>
                    </div>
                </div>
            <div class="details-section">
                <div class="details-grid">
                    <div class="detail-item">
                        <div class="detail-label">Total vendas</div>
                        <div class="detail-value">{total_vendas_ura}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Leads Gerados</div>
                        <div class="detail-value">{telefones_base_ura:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">ticket medio</div>
                        <div class="detail-value">{formatar_real(producao_ura / max(total_vendas_ura, 1))}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Fat. med venda</div>
                        <div class="detail-value">{formatar_real(fat_med_venda)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Produção</div>
                        <div class="detail-value">{formatar_real(producao_ura)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">retorno estimado</div>
                        <div class="detail-value">{formatar_real(retor_estimado)}</div>
                    </div>
                </div>
            </div>
            <div class="roi-section">
                <div class="roi-label">ROI</div>
                <div class="roi-value">{formatar_real(roi_ura)}</div>
            </div>
            """ if uploaded_file is not None else """
            <div style="text-align: center; padding: 40px 20px; color: #888;">
                <div style="font-size: 48px; margin-bottom: 20px;">📁</div>
                <div style="font-size: 18px; margin-bottom: 10px;">Carregue uma base de dados</div>
                <div style="font-size: 14px;">para visualizar as métricas do 4NET</div>
            </div>
            """}
        </div>

        <!-- PAINEL WHATSAPP -->
        <div class="panel">
            <div class="panel-title">PAINEL WHATSAPP</div>
            {f"""
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Mensagens enviadas</div>
                    <div class="metric-value">{campanhas_realizadas}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Interações</div>
                    <div class="metric-value">{camp_atendidas:.1f}%</div>
                </div>
            </div>
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Investimento</div>
                    <div class="metric-value-small">{formatar_real(total_investimento_novo)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Tempo Médio</div>
                    <div class="metric-value-small">{tempo_medio_campanha:.1f}h</div>
                </div>
            </div>
            <div class="details-section">
                <div class="details-grid">
                    <div class="detail-item">
                        <div class="detail-label">Total Vendas</div>
                        <div class="detail-value">{total_vendas_novo:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Produção</div>
                        <div class="detail-value">{formatar_real(producao_novo)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Leads Gerados</div>
                        <div class="detail-value">{total_engajados:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Ticket Médio</div>
                        <div class="detail-value">{formatar_real(producao_novo/total_vendas_novo if total_vendas_novo > 0 else 0)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Retorno Estimado</div>
                        <div class="detail-value">{formatar_real(producao_novo * 0.171)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Fat. Med Venda</div>
                        <div class="detail-value">{formatar_real(producao_novo/total_vendas_novo if total_vendas_novo > 0 else 0)}</div>
                    </div>
                </div>
            </div>
            <div class="roi-section">
                <div class="roi-label">ROI</div>
                <div class="roi-value">{formatar_real(roi_novo)}</div>
            </div>
            """ if uploaded_file is not None else """
            <div style="text-align: center; padding: 40px 20px; color: #888;">
                <div style="font-size: 48px; margin-bottom: 20px;">📱</div>
                <div style="font-size: 18px; margin-bottom: 10px;">Carregue uma base de dados</div>
                <div style="font-size: 14px;">para visualizar as métricas do WhatsApp</div>
            </div>
            """}
        </div>

        <!-- PAINEL AD -->
        <div class="panel">
            <div class="panel-title">PAINEL AD</div>
            {f"""
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Ações</div>
                    <div class="metric-value">{acoes_realizadas}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Efetivas</div>
                    <div class="metric-value">{acoes_efetivas:.1f}%</div>
                </div>
            </div>
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Investimento</div>
                    <div class="metric-value-small">{formatar_real(total_investimento_segundo)}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Tempo Médio</div>
                    <div class="metric-value-small">{tempo_medio_acao:.1f}h</div>
                </div>
            </div>
            <div class="details-section">
                <div class="details-grid">
                    <div class="detail-item">
                        <div class="detail-label">Leads gerados</div>
                        <div class="detail-value">{total_efetivos:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Total Vendas</div>
                        <div class="detail-value">{total_vendas_segundo:,}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Produção</div>
                        <div class="detail-value">{formatar_real(producao_segundo)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Ticket Médio</div>
                        <div class="detail-value">{formatar_real(producao_segundo/total_vendas_segundo if total_vendas_segundo > 0 else 0)}</div>
                    </div>
                </div>
            </div>
            <div class="roi-section">
                <div class="roi-label">ROI</div>
                <div class="roi-value">{formatar_real(roi_segundo)}</div>
            </div>
            """ if uploaded_file is not None else """
            <div style="text-align: center; padding: 40px 20px; color: #888;">
                <div style="font-size: 48px; margin-bottom: 20px;">📢</div>
                <div style="font-size: 18px; margin-bottom: 10px;">Carregue uma base de dados</div>
                <div style="font-size: 14px;">para visualizar as métricas do AD</div>
            </div>
            """}
        </div>
    </div>
    """
    
    components.html(dashboard_html, height=800)
    
    # Inicializar variáveis
    total_leads_gerados = 0
    telefones_base = 0
            
    taxa_ativacao = 0.0
    total_vendas = 0.0
    total_leads_venda = 0.0
    lig_venda = 0.0
    leads_vendas = 0.0
    producao_ura = 0.0
    ticket_ura = 0.0
    roi_ura = 0.0
    retorno_estimado = 0.0
    custo_por_lead = 0.0
    custo_venda = 0.0
    media_venda = 0.0

    # Upload de base local (seção de comparação)
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            
            # Extrair telefones da base carregada (com filtro de data) - usar variável diferente
            telefones_base_todos = extrair_telefones_da_base(df_base, data_ini, data_fim)
            
            # Extrair CPFs da base carregada (com filtro de data)
            cpfs_base = extrair_cpfs_da_base(df_base, data_ini, data_fim)
            
            # Extrair telefones do Kolmeya (usando os dados filtrados por data)
            telefones_kolmeya = extrair_telefones_kolmeya(messages)
            
            # Extrair CPFs do Kolmeya (usando os dados filtrados por data)
            cpfs_kolmeya = extrair_cpfs_kolmeya(messages)
            
            # CALCULAR LEADS GERADOS BASEADO NA COMPARAÇÃO BASE VS KOLMEYA
            if messages:
                # Calcular telefones coincidentes (iguais) entre base e Kolmeya
                telefones_coincidentes = set(telefones_base_todos) & set(telefones_kolmeya)
                total_leads_gerados = len(telefones_coincidentes)
                telefones_base = total_leads_gerados
                
                print(f"🔍 Leads Gerados - Base: {len(telefones_base_todos)}, Kolmeya: {len(telefones_kolmeya)}, Coincidentes: {total_leads_gerados}")
            else:
                # Se não há mensagens do Kolmeya, usar apenas dados da URA
                if centro_custo_selecionado == "Novo":
                    total_leads_gerados = ura_por_status.get('Novo', 0)
                elif centro_custo_selecionado == "FGTS":
                    total_leads_gerados = ura_por_status.get('FGTS', 0)
                elif centro_custo_selecionado == "Crédito CLT":
                    total_leads_gerados = ura_por_status.get('CLT', 0)
                else:
                    total_leads_gerados = ura_count
                telefones_base = total_leads_gerados
            


        except Exception:
            pass
    else:
        # Se não há base carregada, usar apenas dados da URA (painel 4NET)
        if centro_custo_selecionado == "Novo":
            total_leads_gerados = ura_por_status.get('Novo', 0)
        elif centro_custo_selecionado == "FGTS":
            total_leads_gerados = ura_por_status.get('FGTS', 0)
        elif centro_custo_selecionado == "Crédito CLT":
            total_leads_gerados = ura_por_status.get('CLT', 0)
        else:
            total_leads_gerados = ura_count
        telefones_base = total_leads_gerados

def test_environment_status():
    """Função para testar e mostrar status do ambiente."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Status do Ambiente")
    
    # Verificar variáveis de ambiente
    is_render = os.getenv('RENDER', False)
    database_url = os.getenv('DATABASE_URL', 'Não definido')
    
    if is_render:
        st.sidebar.success("🌐 **AMBIENTE: RENDER (Nuvem)**")
        st.sidebar.info(f"📊 Banco: PostgreSQL")
        st.sidebar.info(f"🔗 URL: {database_url[:30]}..." if len(database_url) > 30 else f"🔗 URL: {database_url}")
    else:
        st.sidebar.warning("🏠 **AMBIENTE: LOCAL**")
        st.sidebar.info("📊 Banco: SQLite (dashboard.db)")
    
    # Testar banco de dados
    if HAS_DATABASE:
        try:
            from database_manager import DashboardDatabase
            db = DashboardDatabase()
            
            if db.db_type == 'postgresql':
                st.sidebar.success("✅ Conectado ao PostgreSQL")
            else:
                st.sidebar.success("✅ Conectado ao SQLite")
            
            # Mostrar estatísticas
            stats = db.obter_estatisticas_gerais()
            if stats:
                st.sidebar.markdown("---")
                st.sidebar.markdown("#### 📈 Estatísticas do Banco")
                st.sidebar.metric("Métricas", stats.get('total_metricas', 0))
                st.sidebar.metric("Consultas", stats.get('total_consultas', 0))
                st.sidebar.metric("Tamanho", f"{stats.get('tamanho_banco_mb', 0)} MB")
                
                if stats.get('ultima_atualizacao'):
                    st.sidebar.caption(f"Última atualização: {stats['ultima_atualizacao']}")
        except Exception as e:
            st.sidebar.error(f"❌ Erro no banco: {str(e)[:50]}...")
    else:
        st.sidebar.error("❌ Módulo de banco não encontrado")
    
    # Botão para teste manual
    if st.sidebar.button("🧪 Teste Manual do Banco"):
        try:
            from database_manager import DashboardDatabase
            db = DashboardDatabase()
            
            dados_teste = {
                'canal': 'TESTE_DASHBOARD',
                'sms_enviados': 100,
                'interacoes': 10.5,
                'investimento': 8.0,
                'taxa_entrega': 95.0,
                'total_vendas': 5,
                'producao': 25000.0,
                'leads_gerados': 20,
                'ticket_medio': 5000.0,
                'roi': 24992.0
            }
            
            sucesso = db.salvar_metricas(dados_teste, "TESTE", datetime.now(), datetime.now())
            
            if sucesso:
                st.sidebar.success("✅ Teste salvo com sucesso!")
                st.sidebar.balloons()
            else:
                st.sidebar.error("❌ Erro no teste")
                
        except Exception as e:
            st.sidebar.error(f"❌ Erro: {str(e)[:50]}...")
    
    # Botão para teste do saldo Kolmeya
    if st.sidebar.button("💰 Teste Saldo Kolmeya"):
        try:
            saldo = obter_saldo_kolmeya()
            if saldo > 0:
                st.sidebar.success(f"✅ Saldo: {formatar_real(saldo)}")
            else:
                st.sidebar.warning("⚠️ Saldo zero ou erro na consulta")
        except Exception as e:
            st.sidebar.error(f"❌ Erro: {str(e)[:50]}...")

if __name__ == "__main__":
    main()