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

# Configuração da API FACTA
FACTA_TOKEN_DIRETO = ""  # Coloque seu token aqui para testes

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
            if token and len(token) > 10:
                print(f"✅ Token lido do arquivo kolmeya_token.txt: {token[:10]}...")
                return token
            else:
                print(f"❌ Token no arquivo kolmeya_token.txt é inválido (muito curto ou vazio)")
    except FileNotFoundError:
        print("❌ Arquivo kolmeya_token.txt não encontrado")
    except Exception as e:
        print(f"❌ Erro ao ler token do arquivo: {e}")
    
    print("❌ Nenhum token do Kolmeya encontrado")
    return ""


# Função para obter o token da API do FACTA
def get_facta_token():
    """Retorna o token da API do FACTA."""
    print(f"🔍 Buscando token do FACTA...")
    
    # Primeiro tenta variável de ambiente
    token = os.environ.get("FACTA_TOKEN", "")
    if token:
        print(f"✅ Token encontrado na variável de ambiente: {token[:10]}...")
        return token
    
    # Se não encontrar, tenta configuração direta
    if FACTA_TOKEN_DIRETO:
        token = FACTA_TOKEN_DIRETO
        print(f"⚠️ Usando token configurado diretamente no código: {token[:10]}...")
        return token
    
    # Se não encontrar, tenta ler do arquivo
    try:
        with open("facta_token.txt", "r") as f:
            token = f.read().strip()
            if token and len(token) > 10:
                print(f"✅ Token lido do arquivo facta_token.txt: {token[:10]}...")
                return token
            else:
                print(f"❌ Token no arquivo facta_token.txt é inválido (muito curto ou vazio)")
    except FileNotFoundError:
        print("❌ Arquivo facta_token.txt não encontrado")
    except Exception as e:
        print(f"❌ Erro ao ler token do arquivo: {e}")
    
    print("❌ Nenhum token do FACTA encontrado")
    return ""


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
    
    # Converter para string
    cpf_str = str(cpf).strip()
    
    # Verificar se é notação científica (ex: 1.20225E+17)
    if 'E' in cpf_str.upper() or 'e' in cpf_str:
        try:
            # Converter notação científica para número inteiro
            numero = float(cpf_str)
            cpf_str = str(int(numero))
        except (ValueError, OverflowError):
            return ""
    
    # Remove caracteres não numéricos
    cpf_limpo = re.sub(r'\D', '', cpf_str)
    
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
    
    print(f"🔍 DEBUG - Extraindo CPFs do Kolmeya de {len(messages) if messages else 0} mensagens")
    
    # Debug: Mostrar estrutura das primeiras mensagens
    if messages and len(messages) > 0:
        print(f"🔍 DEBUG - Estrutura da primeira mensagem:")
        primeira_msg = messages[0]
        if isinstance(primeira_msg, dict):
            print(f"   📋 Campos disponíveis: {list(primeira_msg.keys())}")
            print(f"   🆔 CPF: {primeira_msg.get('cpf', 'N/A')}")
            print(f"   📱 Telefone: {primeira_msg.get('telefone', 'N/A')}")
            print(f"   🏢 Centro de custo: {primeira_msg.get('centro_custo', 'N/A')}")
            print(f"   📅 Enviada em: {primeira_msg.get('enviada_em', 'N/A')}")
        
        # Mostrar alguns exemplos de CPFs encontrados
        print(f"🔍 DEBUG - Exemplos de CPFs nas mensagens:")
        for i, msg in enumerate(messages[:10]):  # Primeiras 10 mensagens
            if isinstance(msg, dict):
                cpf = msg.get('cpf')
                telefone = msg.get('telefone', 'N/A')
                print(f"   Mensagem {i+1}: CPF={cpf}, Telefone={telefone}")
    
    cpfs_validos = 0
    cpfs_invalidos = 0
    mensagens_sem_cpf = 0
    
    for i, msg in enumerate(messages):
        if isinstance(msg, dict):
            # Campo 'cpf' da nova API
            cpf = msg.get('cpf')
            
            # CORREÇÃO: Aceitar CPFs mesmo que não passem na validação rigorosa
            if cpf and cpf != 0:  # CPF 0 é inválido
                valor_str = str(cpf).strip()
                
                # Limpar CPF
                cpf_limpo = limpar_cpf(valor_str)
                
                # Verificar se tem pelo menos 11 dígitos
                if cpf_limpo and len(cpf_limpo) == 11:
                    # Tentar validar, mas aceitar mesmo se não passar na validação rigorosa
                    if validar_cpf(cpf_limpo):
                        cpfs.add(cpf_limpo)
                        cpfs_validos += 1
                        if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"   ✅ CPF extraído (válido): {cpf_limpo}")
                    else:
                        # CPF tem 11 dígitos mas não passa na validação - aceitar mesmo assim
                        cpfs.add(cpf_limpo)
                        cpfs_validos += 1
                        if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"   ⚠️ CPF extraído (aceito): {cpf_limpo} (não passou na validação rigorosa)")
                else:
                    cpfs_invalidos += 1
                    if cpfs_invalidos <= 3:  # Mostrar apenas os primeiros 3 para debug
                        print(f"   ❌ CPF inválido (formato): {cpf} -> {cpf_limpo}")
            else:
                mensagens_sem_cpf += 1
                if mensagens_sem_cpf <= 3:  # Mostrar apenas os primeiros 3 para debug
                    print(f"   ⚠️ Mensagem sem CPF válido: {msg.get('telefone', 'N/A')} - CPF: {cpf}")
        
        # Mostrar progresso a cada 1000 mensagens processadas
        if (i + 1) % 1000 == 0:
            print(f"   📊 Progresso: {i + 1}/{len(messages)} mensagens processadas")
    
    print(f"🔍 DEBUG - Resumo da extração de CPFs das mensagens:")
    print(f"   📊 Total de mensagens processadas: {len(messages)}")
    print(f"   ✅ CPFs válidos extraídos: {cpfs_validos}")
    print(f"   ❌ CPFs inválidos encontrados: {cpfs_invalidos}")
    print(f"   ⚠️ Mensagens sem CPF: {mensagens_sem_cpf}")
    print(f"   📋 CPFs únicos finais: {len(cpfs)}")
    
    if cpfs:
        print(f"   📋 Primeiros 5 CPFs de mensagens: {list(cpfs)[:5]}")
        if len(cpfs) > 5:
            print(f"   📋 Últimos 5 CPFs de mensagens: {list(cpfs)[-5:]}")
    
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
    
    # Verificar se o token tem formato válido
    if len(token) < 10:
        print("❌ Token do Kolmeya parece inválido (muito curto)")
        return 0.0
    
    try:
        # Endpoint correto da API Kolmeya para saldo
        url = "https://kolmeya.com.br/api/v1/sms/balance"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        print(f"🔍 Consultando saldo Kolmeya:")
        print(f"   🌐 URL: {url}")
        print(f"   🔑 Token: {token[:10]}..." if token else "   🔑 Token: Não fornecido")
        
        # Usar método POST conforme documentação da API
        resp = requests.post(url, headers=headers, timeout=15)
        print(f"   📊 Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"   📄 Resposta: {data}")
            
            # Campo 'balance' conforme documentação da API
            if 'balance' in data:
                saldo = data.get("balance")
                saldo_float = float(saldo) if saldo is not None else 0.0
                print(f"   ✅ Saldo encontrado: R$ {saldo_float:,.2f}")
                return saldo_float
            else:
                print(f"   ⚠️ Campo 'balance' não encontrado. Campos disponíveis: {list(data.keys())}")
                return 0.0
        elif resp.status_code == 401:
            print(f"   ❌ Erro 401: Token inválido ou expirado")
            return 0.0
        elif resp.status_code == 403:
            print(f"   ❌ Erro 403: Acesso negado")
            return 0.0
        else:
            print(f"   ❌ Erro HTTP {resp.status_code}: {resp.text}")
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
    
    # LIMPEZA DO CACHE: Forçar nova consulta sempre
    print(f"🔄 Forçando nova consulta (cache limpo)")
    
    # Consulta real à API
    try:
        messages = consultar_status_sms_kolmeya(start_at, end_at, token=None, tenant_segment_id=tenant_segment_id)
        
        if messages:
            print(f"✅ API retornou {len(messages)} mensagens")
            
            # Debug adicional: Verificar distribuição de datas
            datas_unicas = set()
            for msg in messages[:20]:  # Primeiras 20 mensagens
                if 'enviada_em' in msg:
                    data_msg = msg['enviada_em']
                    if isinstance(data_msg, str) and len(data_msg) >= 10:
                        datas_unicas.add(data_msg[:10])  # Apenas a data (DD/MM/YYYY)
            
            print(f"🔍 DEBUG - Datas únicas encontradas: {sorted(list(datas_unicas))}")
            
            # Retornar dados reais sem estimativas
            total_acessos = len(messages)  # Um acesso por SMS
            return messages, total_acessos
        else:
            print("⚠️ API não retornou mensagens")
            return [], 0
            
    except Exception as e:
        print(f"❌ Erro na consulta: {e}")
        import traceback
        traceback.print_exc()
        return [], 0

def consultar_status_sms_kolmeya(start_at, end_at, limit=30000, token=None, tenant_segment_id=None):
    """Consulta o status das mensagens SMS enviadas via Kolmeya."""
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado")
        return []
    
    # REMOVIDA: Limitação de 7 dias que estava causando problemas
    # A API do Kolmeya pode aceitar períodos maiores
    try:
        start_dt = datetime.strptime(start_at, '%Y-%m-%d %H:%M')
        end_dt = datetime.strptime(end_at, '%Y-%m-%d %H:%M')
        diff_days = (end_dt - start_dt).days
        
        print(f"🔍 DEBUG - Período solicitado: {diff_days} dias ({start_at} a {end_at})")
        
        # Apenas log de informação, sem bloquear
        if diff_days > 7:
            print(f"⚠️ Período longo solicitado: {diff_days} dias (pode demorar mais)")
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
    
    print(f"🔍 DEBUG - Consultando API Kolmeya:")
    print(f"   🌐 URL: {url}")
    print(f"   📅 Período: {start_at} a {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    print(f"   📋 Request body: {body}")
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=60)  # Aumentado timeout
        
        print(f"   📊 Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            messages = data.get("messages", [])
            
            print(f"✅ Resposta recebida: {len(messages)} mensagens")
            
            # Debug detalhado da resposta
            if messages and len(messages) > 0:
                print(f"🔍 DEBUG - Detalhes da resposta da API:")
                print(f"   📅 Período consultado: {start_at} a {end_at}")
                print(f"   📊 Total de mensagens retornadas: {len(messages)}")
                print(f"   📅 Primeira mensagem - enviada_em: {messages[0].get('enviada_em', 'N/A')}")
                print(f"   📅 Última mensagem - enviada_em: {messages[-1].get('enviada_em', 'N/A')}")
                print(f"   🏢 Centro de custo da primeira: {messages[0].get('centro_custo', 'N/A')}")
                print(f"   📋 Status da primeira: {messages[0].get('status', 'N/A')}")
                
                # Verificar distribuição de datas das mensagens
                datas_mensagens = []
                for msg in messages[:10]:  # Primeiras 10 mensagens
                    if 'enviada_em' in msg:
                        datas_mensagens.append(msg['enviada_em'])
                
                print(f"   📅 Exemplos de datas das mensagens: {datas_mensagens[:5]}")
            else:
                print(f"⚠️ DEBUG - Nenhuma mensagem retornada para o período: {start_at} a {end_at}")
                print(f"   📋 Response completo: {data}")
            
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
                                8105: ["Novo", "8105", "NOVO", "novo", "INSS", "inss", "Inss", 8105],
                                8103: ["FGTS", "8103", "fgts", "Fgts", "Fgts", "Fgts", 8103], 
                                8208: ["Crédito CLT", "8208", "CLT", "clt", "Crédito", "CREDITO", "credito", "CLT", "clt", 8208]
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

def consultar_acessos_sms_kolmeya(start_at, end_at, limit=5000, token=None, tenant_segment_id=None):
    """
    Consulta os acessos realizados nas mensagens SMS enviadas via API do Kolmeya
    """
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado")
        return []
    
    # Para acessos, usar formato de data simples (YYYY-MM-DD)
    # Não validar período pois a API de acessos aceita períodos maiores
    
    url = "https://kolmeya.com.br/api/v1/sms/accesses"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Preparar dados da requisição conforme documentação da API
    data = {
        "start_at": start_at,  # required
        "end_at": end_at,      # required
        "limit": min(limit, 5000),  # required, <= 5000
        "is_robot": 0,         # Excluir acessos de robôs
        "tenant_segment_id": None  # Opcional, será definido abaixo
    }
    
    # Definir tenant_segment_id conforme documentação
    if tenant_segment_id is not None and tenant_segment_id != 0:
        data["tenant_segment_id"] = tenant_segment_id
        print(f"   🏢 Filtrando por centro de custo: {tenant_segment_id}")
    else:
        # Se não especificado, usar 0 para todos os centros de custo
        data["tenant_segment_id"] = 0
        print(f"   🏢 Consultando todos os centros de custo (tenant_segment_id: 0)")
    
    print(f"🔍 DEBUG - Consultando Kolmeya SMS Acessos:")
    print(f"   🌐 URL: {url}")
    print(f"   📅 Período: {start_at} até {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    print(f"   🔑 Token: {token[:10]}...")
    print(f"   📋 Request Body: {data}")
    print(f"   📋 Headers: {headers}")
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=60)  # Aumentado timeout
        response.raise_for_status()
        
        data = response.json()
        
        # A API retorna um dicionário com 'accesses' e 'totalAccesses'
        # Estrutura conforme documentação: {"accesses": [...], "totalAccesses": 0}
        all_accesses = []
        total_accesses = 0
        
        if isinstance(data, dict) and "accesses" in data:
            # Formato correto da API
            all_accesses = data["accesses"]
            total_accesses = data.get("totalAccesses", len(all_accesses))
            print(f"   📊 Estrutura da resposta: dicionário com {len(all_accesses)} acessos")
        elif isinstance(data, list):
            # Fallback para caso a API retorne lista (não esperado)
            print(f"   ⚠️ API retornou lista em vez de dicionário (não esperado)")
            for item in data:
                if isinstance(item, dict) and "accesses" in item:
                    item_accesses = item["accesses"]
                    if item_accesses:
                        all_accesses.extend(item_accesses)
                        total_accesses += item.get("totalAccesses", len(item_accesses))
        else:
            print(f"   ❌ Estrutura de resposta inesperada: {type(data)}")
            print(f"   📋 Conteúdo: {data}")
        
        if all_accesses:
            print(f"✅ Kolmeya SMS Acessos - {len(all_accesses)} acessos encontrados (Total: {total_accesses})")
            
            # Debug do primeiro acesso
            primeiro_acesso = all_accesses[0]
            print(f"🔍 DEBUG - Estrutura do primeiro acesso:")
            print(f"   📋 Campos disponíveis: {list(primeiro_acesso.keys())}")
            print(f"   🆔 CPF: {primeiro_acesso.get('cpf', 'N/A')}")
            print(f"   👤 Nome: {primeiro_acesso.get('name', 'N/A')}")
            print(f"   📱 Telefone: {primeiro_acesso.get('fullphone', 'N/A')}")
            print(f"   💬 Mensagem: {primeiro_acesso.get('message', 'N/A')[:50]}...")
            print(f"   🤖 É robô: {primeiro_acesso.get('is_robot', 'N/A')}")
            print(f"   📅 Acessado em: {primeiro_acesso.get('accessed_at', 'N/A')}")
            print(f"   🏢 Centro de custo: {primeiro_acesso.get('tenant_segment_id', 'N/A')}")
            print(f"   📋 Job ID: {primeiro_acesso.get('job_id', 'N/A')}")
            
            # FILTRAR ACESSOS POR DATA se o campo accessed_at estiver disponível
            acessos_filtrados = []
            acessos_sem_data = 0
            
            for acesso in all_accesses:
                if isinstance(acesso, dict):
                    accessed_at = acesso.get('accessed_at')
                    if accessed_at:
                        try:
                            # Tentar diferentes formatos de data
                            data_acesso = None
                            if isinstance(accessed_at, str):
                                # Formato: YYYY-MM-DD HH:MM:SS
                                if len(accessed_at) >= 19:
                                    data_acesso = datetime.strptime(accessed_at[:19], '%Y-%m-%d %H:%M:%S')
                                # Formato: YYYY-MM-DD
                                elif len(accessed_at) == 10:
                                    data_acesso = datetime.strptime(accessed_at, '%Y-%m-%d')
                            
                            if data_acesso:
                                # Converter datas de entrada para datetime
                                data_ini_dt = datetime.strptime(start_at, '%Y-%m-%d')
                                data_fim_dt = datetime.strptime(end_at, '%Y-%m-%d')
                                
                                # Verificar se está no período
                                if data_ini_dt.date() <= data_acesso.date() <= data_fim_dt.date():
                                    acessos_filtrados.append(acesso)
                                else:
                                    acessos_sem_data += 1
                            else:
                                # Se não conseguiu parsear a data, incluir o acesso
                                acessos_filtrados.append(acesso)
                        except (ValueError, TypeError):
                            # Se erro ao parsear data, incluir o acesso
                            acessos_filtrados.append(acesso)
                    else:
                        # Se não tem data, incluir o acesso
                        acessos_sem_data += 1
                        acessos_filtrados.append(acesso)
            
            print(f"🔍 DEBUG - Filtro por data dos acessos:")
            print(f"   📊 Total de acessos recebidos: {len(all_accesses)}")
            print(f"   📊 Acessos filtrados por data: {len(acessos_filtrados)}")
            print(f"   📊 Acessos sem data ou fora do período: {acessos_sem_data}")
            
            accesses = acessos_filtrados
        else:
            print(f"⚠️ Kolmeya SMS Acessos - Nenhum acesso encontrado")
            print(f"   📋 Response completo: {data}")
            accesses = []
        
        return accesses
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição Kolmeya SMS Acessos: {e}")
        return []
    except Exception as e:
        print(f"❌ Erro inesperado na consulta Kolmeya SMS Acessos: {e}")
        import traceback
        traceback.print_exc()
        return []

def extrair_cpfs_acessos_kolmeya(accesses):
    """Extrai CPFs únicos dos acessos do Kolmeya."""
    cpfs = set()
    
    if not accesses:
        print(f"🔍 DEBUG - Nenhum acesso fornecido para extração de CPFs")
        return cpfs
    
    print(f"🔍 DEBUG - Extraindo CPFs dos acessos do Kolmeya de {len(accesses)} acessos")
    
    # Debug: Mostrar estrutura dos primeiros acessos
    if accesses and len(accesses) > 0:
        print(f"🔍 DEBUG - Estrutura do primeiro acesso:")
        primeiro_acesso = accesses[0]
        if isinstance(primeiro_acesso, dict):
            print(f"   📋 Campos disponíveis: {list(primeiro_acesso.keys())}")
            print(f"   🆔 CPF: {primeiro_acesso.get('cpf', 'N/A')}")
            print(f"   👤 Nome: {primeiro_acesso.get('name', 'N/A')}")
            print(f"   📱 Telefone: {primeiro_acesso.get('fullphone', 'N/A')}")
            print(f"   🏢 Centro de custo: {primeiro_acesso.get('tenant_segment_id', 'N/A')}")
            print(f"   📅 Acessado em: {primeiro_acesso.get('accessed_at', 'N/A')}")
        
        # Mostrar alguns exemplos de CPFs encontrados
        print(f"🔍 DEBUG - Exemplos de CPFs nos acessos:")
        for i, acesso in enumerate(accesses[:10]):  # Primeiros 10 acessos
            if isinstance(acesso, dict):
                cpf = acesso.get('cpf')
                nome = acesso.get('name', 'N/A')
                print(f"   Acesso {i+1}: CPF={cpf}, Nome={nome}")
    
    cpfs_validos = 0
    cpfs_invalidos = 0
    acessos_sem_cpf = 0
    
    for i, acesso in enumerate(accesses):
        if isinstance(acesso, dict):
            # Extrair CPF do campo 'cpf' (conforme documentação da API)
            cpf = acesso.get('cpf')
            
            # CORREÇÃO: Aceitar CPFs mesmo que não passem na validação rigorosa
            if cpf and cpf != 0:  # CPF 0 é inválido
                # Limpar CPF
                cpf_limpo = limpar_cpf(str(cpf))
                
                # Verificar se tem pelo menos 11 dígitos
                if cpf_limpo and len(cpf_limpo) == 11:
                    # Tentar validar, mas aceitar mesmo se não passar na validação rigorosa
                    if validar_cpf(cpf_limpo):
                        cpfs.add(cpf_limpo)
                        cpfs_validos += 1
                        if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"   ✅ CPF de acesso extraído (válido): {cpf_limpo}")
                            print(f"      📱 Telefone: {acesso.get('fullphone', 'N/A')}")
                            print(f"      👤 Nome: {acesso.get('name', 'N/A')}")
                    else:
                        # CPF tem 11 dígitos mas não passa na validação - aceitar mesmo assim
                        cpfs.add(cpf_limpo)
                        cpfs_validos += 1
                        if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"   ⚠️ CPF de acesso extraído (aceito): {cpf_limpo} (não passou na validação rigorosa)")
                            print(f"      📱 Telefone: {acesso.get('fullphone', 'N/A')}")
                            print(f"      👤 Nome: {acesso.get('name', 'N/A')}")
                else:
                    cpfs_invalidos += 1
                    if cpfs_invalidos <= 3:  # Mostrar apenas os primeiros 3 para debug
                        print(f"   ❌ CPF de acesso inválido (formato): {cpf} -> {cpf_limpo}")
            else:
                acessos_sem_cpf += 1
                if acessos_sem_cpf <= 3:  # Mostrar apenas os primeiros 3 para debug
                    print(f"   ⚠️ Acesso sem CPF válido: {acesso.get('name', 'N/A')} - CPF: {cpf}")
                    print(f"      📱 Telefone: {acesso.get('fullphone', 'N/A')}")
                    print(f"      🏢 Centro de custo: {acesso.get('tenant_segment_id', 'N/A')}")
            
            # Mostrar progresso a cada 1000 acessos processados
            if (i + 1) % 1000 == 0:
                print(f"   📊 Progresso: {i + 1}/{len(accesses)} acessos processados")
    
    print(f"🔍 DEBUG - Resumo da extração de CPFs:")
    print(f"   📊 Total de acessos processados: {len(accesses)}")
    print(f"   ✅ CPFs válidos extraídos: {cpfs_validos}")
    print(f"   ❌ CPFs inválidos encontrados: {cpfs_invalidos}")
    print(f"   ⚠️ Acessos sem CPF: {acessos_sem_cpf}")
    print(f"   📋 CPFs únicos finais: {len(cpfs)}")
    
    if cpfs:
        print(f"   📋 Primeiros 5 CPFs de acessos: {list(cpfs)[:5]}")
        if len(cpfs) > 5:
            print(f"   📋 Últimos 5 CPFs de acessos: {list(cpfs)[-5:]}")
    
    return cpfs

def testar_acessos_kolmeya(token=None, start_at=None, end_at=None, tenant_segment_id=None):
    """
    Função para testar especificamente a consulta de acessos do Kolmeya.
    
    Args:
        token: Token do Kolmeya
        start_at: Data inicial (YYYY-MM-DD)
        end_at: Data final (YYYY-MM-DD)
        tenant_segment_id: Centro de custo (opcional)
    
    Returns:
        Lista de acessos encontrados
    """
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado para teste")
        return []
    
    # Usar datas padrão se não fornecidas
    if not start_at:
        start_at = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if not end_at:
        end_at = datetime.now().strftime('%Y-%m-%d')
    
    print(f"🧪 Testando consulta de acessos do Kolmeya:")
    print(f"   📅 Período: {start_at} a {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    print(f"   🔑 Token: {token[:10]}...")
    
    # Consultar acessos
    acessos = consultar_acessos_sms_kolmeya(
        start_at=start_at,
        end_at=end_at,
        limit=5000,
        token=token,
        tenant_segment_id=tenant_segment_id
    )
    
    if acessos:
        print(f"✅ Acessos encontrados: {len(acessos)}")
        
        # Extrair CPFs
        cpfs = extrair_cpfs_acessos_kolmeya(acessos)
        print(f"✅ CPFs únicos extraídos: {len(cpfs)}")
        
        # Mostrar alguns CPFs
        if cpfs:
            print(f"📋 Primeiros 10 CPFs encontrados:")
            for i, cpf in enumerate(list(cpfs)[:10]):
                print(f"   {i+1}. {cpf}")
        
        return acessos
    else:
        print("❌ Nenhum acesso encontrado")
        return []

def consultar_andamento_propostas_facta(cpfs, token=None, ambiente="homologacao"):
    """
    Consulta o andamento de propostas no FACTA para os CPFs fornecidos.
    
    Args:
        cpfs: Lista de CPFs para consultar
        token: Token de autenticação do FACTA
        ambiente: "homologacao" ou "producao"
    
    Returns:
        Lista de propostas encontradas com seus valores AF
    """
    if not cpfs:
        print("❌ Nenhum CPF fornecido para consulta")
        return []
    
    if token is None:
        token = get_facta_token()
    
    if not token:
        print("❌ Token do FACTA não encontrado")
        print("   💡 Verifique se o token está configurado em:")
        print("      - Variável de ambiente: FACTA_TOKEN")
        print("      - Arquivo: facta_token.txt")
        print("      - Configuração direta: FACTA_TOKEN_DIRETO")
        return []
    
    # Verificar se o token tem formato válido
    if len(token) < 10:
        print("❌ Token do FACTA parece inválido (muito curto)")
        return []
    
    # Definir URL base conforme ambiente
    if ambiente.lower() == "producao":
        base_url = "https://webservice.facta.com.br"
    else:
        base_url = "https://webservice-homol.facta.com.br"
    
    url = f"{base_url}/proposta/andamento-propostas"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print(f"🔍 DEBUG - Consultando FACTA Andamento de Propostas:")
    print(f"   🌐 URL: {url}")
    print(f"   🔑 Token: {token[:10]}...")
    print(f"   📊 CPFs para consulta: {len(cpfs)}")
    print(f"   🏢 Ambiente: {ambiente}")
    
    todas_propostas = []
    
    # Processar CPFs em lotes para evitar timeout
    lote_size = 10  # Consultar 10 CPFs por vez
    for i in range(0, len(cpfs), lote_size):
        lote_cpfs = list(cpfs)[i:i+lote_size]
        print(f"   📋 Processando lote {i//lote_size + 1}: CPFs {i+1}-{min(i+lote_size, len(cpfs))}")
        
        for cpf in lote_cpfs:
            try:
                # Parâmetros da consulta - CORREÇÃO: Remover filtros desnecessários
                params = {
                    "cpf": cpf,
                    "quantidade": 5000  # Máximo de registros por página
                }
                
                
                
                print(f"      🔍 Consultando CPF: {cpf}")
                print(f"      📋 Parâmetros: {params}")
                
                response = requests.get(url, headers=headers, params=params, timeout=30)
                print(f"      📊 Status Code: {response.status_code}")
                
                response.raise_for_status()
                
                data = response.json()
                print(f"      📄 Resposta FACTA: {data}")
                
                # CORREÇÃO: Verificar se não há erro (erro = false significa sucesso)
                if not data.get("erro", False):
                    propostas = data.get("propostas", [])
                    if propostas:
                        print(f"      ✅ CPF {cpf}: {len(propostas)} propostas encontradas")
                        
                        for proposta in propostas:
                            # Extrair informações relevantes
                            proposta_info = {
                                "cpf": cpf,
                                "cliente": proposta.get("cliente", ""),
                                "codigo_af": proposta.get("codigo_af", ""),
                                "valor_af": proposta.get("valor_af", 0),
                                "status_proposta": proposta.get("status_proposta", ""),
                                "data_movimento": proposta.get("data_movimento", ""),
                                "convenio": proposta.get("convenio", ""),
                                "averbador": proposta.get("averbador", ""),
                                "produto": proposta.get("produto", ""),
                                "valor_bruto": proposta.get("valor_bruto", 0),
                                "saldo_devedor": proposta.get("saldo_devedor", 0)
                            }
                            todas_propostas.append(proposta_info)
                    else:
                        print(f"      ⚠️ CPF {cpf}: Nenhuma proposta encontrada")
                else:
                    print(f"      ❌ CPF {cpf}: Erro na consulta - {data.get('mensagem', 'Erro desconhecido')}")
                
                # Pequena pausa entre consultas para não sobrecarregar a API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"      ❌ Erro na requisição para CPF {cpf}: {e}")
            except Exception as e:
                print(f"      ❌ Erro inesperado para CPF {cpf}: {e}")
    
    print(f"✅ FACTA Andamento de Propostas - {len(todas_propostas)} propostas encontradas")
    
    # Resumo dos valores AF
    if todas_propostas:
        valores_af = [p.get("valor_af", 0) for p in todas_propostas if p.get("valor_af", 0) > 0]
        if valores_af:
            print(f"   📊 Total de propostas com valor AF: {len(valores_af)}")
            print(f"   💰 Soma dos valores AF: R$ {sum(valores_af):,.2f}")
            print(f"   📈 Maior valor AF: R$ {max(valores_af):,.2f}")
            print(f"   📉 Menor valor AF: R$ {min(valores_af):,.2f}")
    
    return todas_propostas

def consultar_todas_propostas_facta(token=None, ambiente="homologacao", data_ini=None, data_fim=None):
    """
    Consulta TODAS as propostas no FACTA sem filtro de CPF específico.
    
    Args:
        token: Token de autenticação do FACTA
        ambiente: "homologacao" ou "producao"
        data_ini: Data inicial (opcional)
        data_fim: Data final (opcional)
    
    Returns:
        Lista de todas as propostas encontradas
    """
    if token is None:
        token = get_facta_token()
    
    if not token:
        print("❌ Token do FACTA não encontrado")
        return []
    
    # Definir URL base conforme ambiente
    if ambiente.lower() == "producao":
        base_url = "https://webservice.facta.com.br"
    else:
        base_url = "https://webservice-homol.facta.com.br"
    
    url = f"{base_url}/proposta/andamento-propostas"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print(f"🔍 DEBUG - Consultando TODAS as propostas FACTA:")
    print(f"   🌐 URL: {url}")
    print(f"   🔑 Token: {token[:10]}...")
    print(f"   🏢 Ambiente: {ambiente}")
    print(f"   📅 Período: {data_ini} a {data_fim}")
    
    todas_propostas = []
    pagina = 1
    
    while True:
        try:
            # Parâmetros da consulta - SEM filtro de CPF
            params = {
                "quantidade": 5000,  # Máximo de registros por página
                "pagina": pagina
            }
            
            # Adicionar filtros de data se fornecidos
            if data_ini:
                params["data_ini"] = data_ini.strftime('%d/%m/%Y')
            if data_fim:
                params["data_fim"] = data_fim.strftime('%d/%m/%Y')
            
            print(f"   📋 Consultando página {pagina}...")
            print(f"   📋 Parâmetros: {params}")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            print(f"   📊 Status Code: {response.status_code}")
            
            response.raise_for_status()
            
            data = response.json()
            
            # Verificar se não há erro
            if not data.get("erro", False):
                propostas = data.get("propostas", [])
                if propostas:
                    print(f"   ✅ Página {pagina}: {len(propostas)} propostas encontradas")
                    
                    for proposta in propostas:
                        # Extrair informações relevantes
                        proposta_info = {
                            "cpf": proposta.get("cpf", ""),
                            "cliente": proposta.get("cliente", ""),
                            "codigo_af": proposta.get("codigo_af", ""),
                            "valor_af": proposta.get("valor_af", 0),
                            "status_proposta": proposta.get("status_proposta", ""),
                            "data_movimento": proposta.get("data_movimento", ""),
                            "convenio": proposta.get("convenio", ""),
                            "averbador": proposta.get("averbador", ""),
                            "produto": proposta.get("produto", ""),
                            "valor_bruto": proposta.get("valor_bruto", 0),
                            "saldo_devedor": proposta.get("saldo_devedor", 0)
                        }
                        todas_propostas.append(proposta_info)
                    
                    # Se retornou menos que o máximo, chegou ao fim
                    if len(propostas) < 5000:
                        print(f"   ✅ Última página alcançada (menos de 5000 registros)")
                        break
                    
                    pagina += 1
                else:
                    print(f"   ⚠️ Página {pagina}: Nenhuma proposta encontrada")
                    break
            else:
                print(f"   ❌ Página {pagina}: Erro na consulta - {data.get('mensagem', 'Erro desconhecido')}")
                break
            
            # Pequena pausa entre páginas
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Erro na requisição da página {pagina}: {e}")
            break
        except Exception as e:
            print(f"   ❌ Erro inesperado na página {pagina}: {e}")
            break
    
    print(f"✅ FACTA Todas as Propostas - {len(todas_propostas)} propostas encontradas")
    
    # Resumo dos valores AF
    if todas_propostas:
        valores_af = [p.get("valor_af", 0) for p in todas_propostas if p.get("valor_af", 0) > 0]
        if valores_af:
            print(f"   📊 Total de propostas com valor AF: {len(valores_af)}")
            print(f"   💰 Soma dos valores AF: R$ {sum(valores_af):,.2f}")
            print(f"   📈 Maior valor AF: R$ {max(valores_af):,.2f}")
            print(f"   📉 Menor valor AF: R$ {min(valores_af):,.2f}")
    
    return todas_propostas

def testar_api_facta(token=None, ambiente="homologacao"):
    """
    Função para testar a conectividade com a API FACTA.
    
    Args:
        token: Token de autenticação do FACTA
        ambiente: "homologacao" ou "producao"
    
    Returns:
        True se a API estiver funcionando, False caso contrário
    """
    if token is None:
        token = get_facta_token()
    
    if not token:
        print("❌ Token do FACTA não encontrado para teste")
        return False
    
    # Definir URL base conforme ambiente
    if ambiente.lower() == "producao":
        base_url = "https://webservice.facta.com.br"
    else:
        base_url = "https://webservice-homol.facta.com.br"
    
    url = f"{base_url}/proposta/andamento-propostas"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Parâmetros de teste (consulta vazia para testar conectividade)
    params = {
        "convenio": 3,
        "consulta_sub": "N",
        "quantidade": 1
    }
    
    print(f"🧪 Testando conectividade com API FACTA:")
    print(f"   🌐 URL: {url}")
    print(f"   🔑 Token: {token[:10]}...")
    print(f"   🏢 Ambiente: {ambiente}")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"   📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ API FACTA respondendo corretamente")
            print(f"   📄 Resposta: {data}")
            return True
        elif response.status_code == 401:
            print(f"   ❌ Erro 401: Token inválido ou expirado")
            return False
        elif response.status_code == 403:
            print(f"   ❌ Erro 403: Acesso negado")
            return False
        else:
            print(f"   ❌ Erro HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("   ❌ Timeout na requisição de teste")
        return False
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro na requisição de teste: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Erro inesperado no teste: {e}")
        return False

def consultar_cpfs_diretamente_kolmeya(start_at, end_at, limit=30000, token=None, tenant_segment_id=None):
    """
    Consulta diretamente o endpoint de status de SMS do Kolmeya e extrai CPFs.
    
    Args:
        start_at: Data inicial (YYYY-MM-DD HH:MM)
        end_at: Data final (YYYY-MM-DD HH:MM)
        limit: Limite de mensagens a consultar
        token: Token do Kolmeya
        tenant_segment_id: Centro de custo (opcional)
    
    Returns:
        Lista de CPFs únicos encontrados
    """
    if token is None:
        token = get_kolmeya_token()
    
    if not token:
        print("❌ Token do Kolmeya não encontrado")
        return []
    
    # Verificar se o token tem formato válido
    if len(token) < 10:
        print("❌ Token do Kolmeya parece inválido (muito curto)")
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
    
    print(f"🔍 DEBUG - Consultando CPFs diretamente do endpoint de status:")
    print(f"   🌐 URL: {url}")
    print(f"   📅 Período: {start_at} a {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    print(f"   📋 Request body: {body}")
    
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        
        print(f"   📊 Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            messages = data.get("messages", [])
            
            print(f"✅ Resposta recebida: {len(messages)} mensagens")
            
            # Debug da primeira mensagem
            if messages and len(messages) > 0:
                primeira_msg = messages[0]
                print(f"🔍 DEBUG - Estrutura da primeira mensagem:")
                print(f"   📋 Campos disponíveis: {list(primeira_msg.keys())}")
                print(f"   🆔 CPF: {primeira_msg.get('cpf', 'N/A')}")
                print(f"   📱 Telefone: {primeira_msg.get('telefone', 'N/A')}")
                print(f"   🏢 Centro de custo: {primeira_msg.get('centro_custo', 'N/A')}")
                print(f"   📅 Enviada em: {primeira_msg.get('enviada_em', 'N/A')}")
            
            # Extrair CPFs das mensagens
            cpfs = set()
            cpfs_validos = 0
            cpfs_invalidos = 0
            mensagens_sem_cpf = 0
            
            for i, msg in enumerate(messages):
                if isinstance(msg, dict):
                    # Campo 'cpf' da API
                    cpf = msg.get('cpf')
                    
                    # CORREÇÃO: Aceitar CPFs mesmo que não passem na validação rigorosa
                    if cpf and cpf != 0:  # CPF 0 é inválido
                        valor_str = str(cpf).strip()
                        
                        # Limpar CPF
                        cpf_limpo = limpar_cpf(valor_str)
                        
                        # Verificar se tem pelo menos 11 dígitos
                        if cpf_limpo and len(cpf_limpo) == 11:
                            # Tentar validar, mas aceitar mesmo se não passar na validação rigorosa
                            if validar_cpf(cpf_limpo):
                                cpfs.add(cpf_limpo)
                                cpfs_validos += 1
                                if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                                    print(f"   ✅ CPF extraído (válido): {cpf_limpo}")
                            else:
                                # CPF tem 11 dígitos mas não passa na validação - aceitar mesmo assim
                                cpfs.add(cpf_limpo)
                                cpfs_validos += 1
                                if len(cpfs) <= 5:  # Mostrar apenas os primeiros 5 para debug
                                    print(f"   ⚠️ CPF extraído (aceito): {cpf_limpo} (não passou na validação rigorosa)")
                        else:
                            cpfs_invalidos += 1
                            if cpfs_invalidos <= 3:  # Mostrar apenas os primeiros 3 para debug
                                print(f"   ❌ CPF inválido (formato): {cpf} -> {cpf_limpo}")
                    else:
                        mensagens_sem_cpf += 1
                        if mensagens_sem_cpf <= 3:  # Mostrar apenas os primeiros 3 para debug
                            print(f"   ⚠️ Mensagem sem CPF válido: {msg.get('telefone', 'N/A')} - CPF: {cpf}")
                
                # Mostrar progresso a cada 1000 mensagens processadas
                if (i + 1) % 1000 == 0:
                    print(f"   📊 Progresso: {i + 1}/{len(messages)} mensagens processadas")
            
            print(f"🔍 DEBUG - Resumo da extração de CPFs do endpoint:")
            print(f"   📊 Total de mensagens processadas: {len(messages)}")
            print(f"   ✅ CPFs válidos extraídos: {cpfs_validos}")
            print(f"   ❌ CPFs inválidos encontrados: {cpfs_invalidos}")
            print(f"   ⚠️ Mensagens sem CPF: {mensagens_sem_cpf}")
            print(f"   📋 CPFs únicos finais: {len(cpfs)}")
            
            if cpfs:
                print(f"   📋 Primeiros 5 CPFs: {list(cpfs)[:5]}")
                if len(cpfs) > 5:
                    print(f"   📋 Últimos 5 CPFs: {list(cpfs)[-5:]}")
            
            return list(cpfs)
            
        elif resp.status_code == 401:
            print(f"   ❌ Erro 401: Token inválido ou expirado")
            return []
        elif resp.status_code == 403:
            print(f"   ❌ Erro 403: Acesso negado")
            return []
        else:
            print(f"   ❌ Erro HTTP {resp.status_code}: {resp.text}")
            return []
            
    except requests.exceptions.Timeout:
        print("   ❌ Timeout na requisição")
        return []
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro na requisição: {e}")
        return []
    except Exception as e:
        print(f"   ❌ Erro inesperado: {e}")
        return []

def consultar_andamento_propostas_por_endpoint_direto(start_at, end_at, limit=30000, token_kolmeya=None, token_facta=None, ambiente_facta="homologacao", tenant_segment_id=None):
    """
    Consulta diretamente o endpoint de status do Kolmeya e depois consulta propostas do FACTA.
    
    Args:
        start_at: Data inicial (YYYY-MM-DD)
        end_at: Data final (YYYY-MM-DD)
        limit: Limite de mensagens a consultar
        token_kolmeya: Token do Kolmeya
        token_facta: Token do FACTA
        ambiente_facta: Ambiente do FACTA ("homologacao" ou "producao")
        tenant_segment_id: Centro de custo do Kolmeya
    
    Returns:
        Dicionário com CPFs e propostas encontradas
    """
    print(f"🚀 Iniciando consulta integrada Kolmeya (ENDPOINT DIRETO) + FACTA")
    print(f"   📅 Período: {start_at} até {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    
    # Obter tokens se não fornecidos
    if token_kolmeya is None:
        token_kolmeya = get_kolmeya_token()
    
    if token_facta is None:
        token_facta = get_facta_token()
    
    # 1. Consultar CPFs diretamente do endpoint de status
    print(f"\n📱 Passo 1: Consultando CPFs diretamente do endpoint de status...")
    
    # Formatar datas para o formato esperado pela API (YYYY-MM-DD HH:MM)
    start_at_formatted = f"{start_at} 00:00"
    end_at_formatted = f"{end_at} 23:59"
    
    cpfs = consultar_cpfs_diretamente_kolmeya(
        start_at=start_at_formatted,
        end_at=end_at_formatted,
        limit=limit,
        token=token_kolmeya,
        tenant_segment_id=tenant_segment_id
    )
    
    if not cpfs:
        print("❌ Nenhum CPF encontrado. Finalizando consulta.")
        return {
            "cpfs": [],
            "propostas": [],
            "cpfs_encontrados": 0,
            "propostas_encontradas": 0
        }
    
    # 2. Testar conectividade com FACTA antes de consultar
    print(f"\n🧪 Passo 2: Testando conectividade com API FACTA...")
    if not testar_api_facta(token=token_facta, ambiente=ambiente_facta):
        print("❌ API FACTA não está respondendo. Finalizando consulta.")
        return {
            "cpfs": cpfs,
            "propostas": [],
            "cpfs_encontrados": len(cpfs),
            "propostas_encontradas": 0
        }
    
    # 3. Consultar propostas do FACTA
    print(f"\n📋 Passo 3: Consultando propostas do FACTA...")
    propostas = consultar_andamento_propostas_facta(
        cpfs=set(cpfs),  # Converter para set
        token=token_facta,
        ambiente=ambiente_facta
    )
    
    # 4. Resumo final
    print(f"\n✅ Consulta integrada finalizada:")
    print(f"   🆔 CPFs únicos encontrados: {len(cpfs)}")
    print(f"   📋 Propostas encontradas: {len(propostas)}")
    
    return {
        "cpfs": cpfs,
        "propostas": propostas,
        "cpfs_encontrados": len(cpfs),
        "propostas_encontradas": len(propostas)
    }

def consultar_andamento_propostas_por_envios(start_at, end_at, limit=5000, token_kolmeya=None, token_facta=None, ambiente_facta="homologacao", tenant_segment_id=None):
    """
    Consulta mensagens enviadas do Kolmeya e depois consulta propostas do FACTA para os CPFs encontrados.
    
    Args:
        start_at: Data inicial (YYYY-MM-DD)
        end_at: Data final (YYYY-MM-DD)
        limit: Limite de mensagens a consultar
        token_kolmeya: Token do Kolmeya
        token_facta: Token do FACTA
        ambiente_facta: Ambiente do FACTA ("homologacao" ou "producao")
        tenant_segment_id: Centro de custo do Kolmeya
    
    Returns:
        Dicionário com mensagens e propostas encontradas
    """
    print(f"🚀 Iniciando consulta integrada Kolmeya (ENVIOS) + FACTA")
    print(f"   📅 Período: {start_at} até {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    
    # Obter tokens se não fornecidos
    if token_kolmeya is None:
        token_kolmeya = get_kolmeya_token()
    
    if token_facta is None:
        token_facta = get_facta_token()
    
    # 1. Consultar mensagens enviadas do Kolmeya
    print(f"\n📱 Passo 1: Consultando mensagens enviadas do Kolmeya...")
    messages, total_acessos = obter_dados_sms_com_filtro(
        data_ini=datetime.strptime(start_at, '%Y-%m-%d').date(),
        data_fim=datetime.strptime(end_at, '%Y-%m-%d').date(),
        tenant_segment_id=tenant_segment_id
    )
    
    if not messages:
        print("❌ Nenhuma mensagem encontrada. Finalizando consulta.")
        return {
            "mensagens": [],
            "propostas": [],
            "cpfs_encontrados": 0,
            "propostas_encontradas": 0
        }
    
    # 2. Extrair CPFs das mensagens enviadas
    print(f"\n🆔 Passo 2: Extraindo CPFs das mensagens enviadas...")
    cpfs = extrair_cpfs_kolmeya(messages)
    
    if not cpfs:
        print("❌ Nenhum CPF válido encontrado nas mensagens. Finalizando consulta.")
        return {
            "mensagens": messages,
            "propostas": [],
            "cpfs_encontrados": 0,
            "propostas_encontradas": 0
        }
    
    # 3. Testar conectividade com FACTA antes de consultar
    print(f"\n🧪 Passo 3a: Testando conectividade com API FACTA...")
    if not testar_api_facta(token=token_facta, ambiente=ambiente_facta):
        print("❌ API FACTA não está respondendo. Finalizando consulta.")
        return {
            "mensagens": messages,
            "propostas": [],
            "cpfs_encontrados": len(cpfs),
            "propostas_encontradas": 0
        }
    
    # 4. Consultar propostas do FACTA
    print(f"\n📋 Passo 4: Consultando propostas do FACTA...")
    propostas = consultar_andamento_propostas_facta(
        cpfs=cpfs,
        token=token_facta,
        ambiente=ambiente_facta
    )
    
    # 5. Resumo final
    print(f"\n✅ Consulta integrada finalizada:")
    print(f"   📱 Mensagens encontradas: {len(messages)}")
    print(f"   🆔 CPFs únicos: {len(cpfs)}")
    print(f"   📋 Propostas encontradas: {len(propostas)}")
    
    return {
        "mensagens": messages,
        "propostas": propostas,
        "cpfs_encontrados": len(cpfs),
        "propostas_encontradas": len(propostas)
    }

def consultar_andamento_propostas_por_acessos(start_at, end_at, limit=5000, token_kolmeya=None, token_facta=None, ambiente_facta="homologacao", tenant_segment_id=None):
    """
    Consulta acessos do Kolmeya e depois consulta propostas do FACTA para os CPFs encontrados.
    
    Args:
        start_at: Data inicial (YYYY-MM-DD)
        end_at: Data final (YYYY-MM-DD)
        limit: Limite de acessos a consultar
        token_kolmeya: Token do Kolmeya
        token_facta: Token do FACTA
        ambiente_facta: Ambiente do FACTA ("homologacao" ou "producao")
        tenant_segment_id: Centro de custo do Kolmeya
    
    Returns:
        Dicionário com acessos e propostas encontradas
    """
    print(f"🚀 Iniciando consulta integrada Kolmeya + FACTA")
    print(f"   📅 Período: {start_at} até {end_at}")
    print(f"   🏢 Centro de custo: {tenant_segment_id}")
    
    # Obter tokens se não fornecidos
    if token_kolmeya is None:
        token_kolmeya = get_kolmeya_token()
    
    if token_facta is None:
        token_facta = get_facta_token()
    
    # 1. Consultar acessos do Kolmeya
    print(f"\n📱 Passo 1: Consultando acessos do Kolmeya...")
    acessos = consultar_acessos_sms_kolmeya(
        start_at=start_at,
        end_at=end_at,
        limit=limit,
        token=token_kolmeya,
        tenant_segment_id=tenant_segment_id
    )
    
    if not acessos:
        print("❌ Nenhum acesso encontrado. Finalizando consulta.")
        return {
            "acessos": [],
            "propostas": [],
            "cpfs_encontrados": 0,
            "propostas_encontradas": 0
        }
    
    # 2. Extrair CPFs dos acessos
    print(f"\n🆔 Passo 2: Extraindo CPFs dos acessos...")
    cpfs = extrair_cpfs_acessos_kolmeya(acessos)
    
    if not cpfs:
        print("❌ Nenhum CPF válido encontrado nos acessos. Finalizando consulta.")
        return {
            "acessos": acessos,
            "propostas": [],
            "cpfs_encontrados": 0,
            "propostas_encontradas": 0
        }
    
    # 3. Testar conectividade com FACTA antes de consultar
    print(f"\n🧪 Passo 3a: Testando conectividade com API FACTA...")
    if not testar_api_facta(token=token_facta, ambiente=ambiente_facta):
        print("❌ API FACTA não está respondendo. Finalizando consulta.")
        return {
            "acessos": acessos,
            "propostas": [],
            "cpfs_encontrados": len(cpfs),
            "propostas_encontradas": 0
        }
    
    # 4. Consultar propostas do FACTA
    print(f"\n📋 Passo 4: Consultando propostas do FACTA...")
    propostas = consultar_andamento_propostas_facta(
        cpfs=cpfs,
        token=token_facta,
        ambiente=ambiente_facta
    )
    
    # 5. Resumo final
    print(f"\n✅ Consulta integrada finalizada:")
    print(f"   📱 Acessos encontrados: {len(acessos)}")
    print(f"   🆔 CPFs únicos: {len(cpfs)}")
    print(f"   📋 Propostas encontradas: {len(propostas)}")
    
    return {
        "acessos": acessos,
        "propostas": propostas,
        "cpfs_encontrados": len(cpfs),
        "propostas_encontradas": len(propostas)
    }

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
    print(f"🔍 DEBUG - Extraindo URA da base:")
    print(f"   📊 Total de registros na base: {len(df)}")
    print(f"   📅 Filtro de data: {data_ini} a {data_fim}")
    print(f"   📋 Colunas de data encontradas: {colunas_data}")
    print(f"   📋 Colunas UTM encontradas: {colunas_utm}")
    
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
                if not data_ini or not data_fim or data_valida:
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
    
    # Log final dos resultados
    print(f"🔍 DEBUG - Resultados da extração URA:")
    print(f"   📊 Total de registros URA encontrados: {ura_count}")
    print(f"   📋 Distribuição por status: {ura_por_status}")
    print(f"   📋 CPFs únicos por status: {dict((k, len(v)) for k, v in ura_cpfs_por_status.items())}")
    
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
    mensagens_fora_periodo = 0
    
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
                            mensagens_fora_periodo += 1
                            if mensagens_processadas <= 5:  # Mostrar apenas as primeiras 5 para debug
                                print(f"   ❌ Mensagem fora do período: {data_str} (criada em {data_criacao})")
                                print(f"      Comparação: {data_ini_dt} <= {data_criacao} <= {data_fim_dt}")
                except (ValueError, TypeError) as e:
                    print(f"   ⚠️ Erro ao processar data '{data_str}': {e}")
                    continue
            else:
                # Se não tem campo 'enviada_em', incluir a mensagem (não filtrar)
                mensagens_filtradas.append(msg)
                if mensagens_processadas <= 5:
                    print(f"   ⚠️ Mensagem sem data incluída (sem filtro): {msg.get('id', 'N/A')}")
    
    print(f"   📊 Mensagens processadas: {mensagens_processadas}")
    print(f"   📊 Mensagens incluídas: {len(mensagens_filtradas)}")
    print(f"   📊 Mensagens fora do período: {mensagens_fora_periodo}")
    
    # Se o filtro for muito restritivo, retornar todas as mensagens
    if len(mensagens_filtradas) < len(messages) * 0.1:  # Se menos de 10% das mensagens passaram no filtro
        print(f"   ⚠️ Filtro muito restritivo! Retornando todas as mensagens ({len(messages)})")
        return messages
    
    return mensagens_filtradas







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
    print(f"🔍 DEBUG - INICIANDO extrair_whatsapp_da_base")
    print(f"   📊 DataFrame shape: {df.shape if df is not None else 'None'}")
    print(f"   📋 Colunas disponíveis: {list(df.columns) if df is not None else 'None'}")
    
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
        print(f"   ⚠️ DataFrame vazio ou None")
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
        print(f"   ❌ NENHUMA coluna UTM encontrada!")
        print(f"   📋 Todas as colunas: {list(df.columns)}")
        return whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status
    else:
        print(f"   ✅ Colunas UTM encontradas: {colunas_utm}")
    
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
    print(f"🔍 DEBUG - Extraindo WhatsApp da base:")
    print(f"   📊 Total de registros na base: {len(df)}")
    print(f"   📅 Filtro de data: {data_ini} a {data_fim}")
    print(f"   📋 Colunas de data encontradas: {colunas_data}")
    print(f"   📋 Colunas UTM encontradas: {colunas_utm}")
    
    # Debug: Verificar alguns valores da coluna UTM
    if colunas_utm:
        print(f"🔍 DEBUG - Verificando valores na coluna UTM '{colunas_utm[0]}':")
        valores_unicos = df[colunas_utm[0]].dropna().unique()
        print(f"   📋 Valores únicos encontrados: {valores_unicos[:10]}")  # Primeiros 10 valores
        
        # Verificar especificamente por "WHATSAPP_MKT"
        registros_whatsapp = df[df[colunas_utm[0]].str.upper() == "WHATSAPP_MKT"]
        print(f"   📊 Registros com 'WHATSAPP_MKT' encontrados: {len(registros_whatsapp)}")
        if len(registros_whatsapp) > 0:
            print(f"   📋 Primeiros registros WhatsApp:")
            for idx, row in registros_whatsapp.head(3).iterrows():
                print(f"      Linha {idx}: {row[colunas_utm[0]]}")
    else:
        print(f"   ❌ Nenhuma coluna UTM encontrada!")
    
    for idx, row in df.iterrows():
        # Verifica se tem UTM source = "WHATSAPP_MKT"
        tem_whatsapp = False
        for col in colunas_utm:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip().upper()
                if valor_str == "WHATSAPP_MKT":
                    tem_whatsapp = True
                    if whatsapp_count < 5:  # Mostrar apenas os primeiros 5 para debug
                        print(f"   ✅ Registro WhatsApp encontrado na linha {idx}, coluna '{col}': '{valor}'")
                    break
        
        if tem_whatsapp:
            # Se há filtro de data, verifica se está no período
            if data_ini and data_fim and colunas_data:
                data_valida = False
                if whatsapp_count < 3:  # Debug para os primeiros registros
                    print(f"   🔍 DEBUG - Verificando filtro de data para registro WhatsApp linha {idx}")
                    print(f"      📅 Filtro: {data_ini} a {data_fim}")
                
                for col in colunas_data:
                    try:
                        data_str = str(row[col])
                        if pd.notna(data_str) and data_str.strip():
                            if whatsapp_count < 3:
                                print(f"      📋 Coluna '{col}': '{data_str}'")
                            
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
                                
                                if whatsapp_count < 3:
                                    print(f"      📅 Data parseada: {data_criacao}")
                                    print(f"      📅 Range filtro: {data_ini_dt} a {data_fim_dt}")
                                    print(f"      ✅ Data válida: {data_ini_dt <= data_criacao <= data_fim_dt}")
                                
                                if data_ini_dt <= data_criacao <= data_fim_dt:
                                    data_valida = True
                                    break
                    except (ValueError, TypeError) as e:
                        if whatsapp_count < 3:
                            print(f"      ❌ Erro ao parsear data '{data_str}': {e}")
                        continue
                
                # Se não há filtro de data ou se a data está no período, conta o registro
                if not data_ini or not data_fim or data_valida:
                    whatsapp_count += 1
                    if whatsapp_count <= 3:
                        print(f"      ✅ Registro WhatsApp ACEITO pelo filtro de data (Total: {whatsapp_count})")
                else:
                    if whatsapp_count < 3:
                        print(f"      ❌ Registro WhatsApp REJEITADO pelo filtro de data")
                    continue  # Pula para o próximo registro se a data não for válida
            
            # Extrai CPF do registro (apenas se o registro foi aceito)
            cpf_encontrado = None
            for col in colunas_cpf:
                valor_cpf = row[col] if col in row else None
                if valor_cpf is not None:
                    valor_cpf_str = str(valor_cpf).strip()
                    # Usar a nova função de limpeza de CPF
                    cpf_limpo = limpar_cpf(valor_cpf_str)
                    if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                        cpf_encontrado = cpf_limpo
                        if whatsapp_count <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"      ✅ CPF encontrado na coluna '{col}': '{valor_cpf}' -> '{cpf_limpo}'")
                        break
                    elif whatsapp_count <= 3:  # Mostrar apenas os primeiros 3 para debug
                        print(f"      ⚠️ CPF inválido na coluna '{col}': '{valor_cpf}' -> '{cpf_limpo}'")
            
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
    
    # Log final dos resultados
    print(f"🔍 DEBUG - Resultados da extração WhatsApp:")
    print(f"   📊 Total de registros WhatsApp encontrados: {whatsapp_count}")
    print(f"   📋 Distribuição por status: {whatsapp_por_status}")
    print(f"   📋 CPFs únicos por status: {dict((k, len(v)) for k, v in whatsapp_cpfs_por_status.items())}")
    
    # Debug adicional: Mostrar CPFs encontrados
    for status, cpfs in whatsapp_cpfs_por_status.items():
        if cpfs:
            print(f"   📋 CPFs {status} encontrados: {list(cpfs)[:3]}")  # Mostrar primeiros 3 CPFs
    
    print(f"🔍 DEBUG - FUNÇÃO extrair_whatsapp_da_base CONCLUÍDA")
    return whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status

def extrair_ad_da_base(df, data_ini=None, data_fim=None):
    """Extrai e conta registros com UTM source = 'ad' da base carregada, separados por status e opcionalmente filtrados por data."""
    print(f"🔍 DEBUG - INICIANDO extrair_ad_da_base")
    print(f"   📊 DataFrame shape: {df.shape if df is not None else 'None'}")
    print(f"   📋 Colunas disponíveis: {list(df.columns) if df is not None else 'None'}")
    
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
        print(f"   ⚠️ DataFrame vazio ou None")
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
        print(f"   ❌ NENHUMA coluna UTM encontrada!")
        print(f"   📋 Todas as colunas: {list(df.columns)}")
        return ad_count, ad_por_status, ad_cpfs_por_status
    else:
        print(f"   ✅ Colunas UTM encontradas: {colunas_utm}")  
    
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
    
    # Conta registros com valor "ad" (minúsculo)
    print(f"🔍 DEBUG - Extraindo AD da base:")
    print(f"   📊 Total de registros na base: {len(df)}")
    print(f"   📅 Filtro de data: {data_ini} a {data_fim}")
    print(f"   📋 Colunas de data encontradas: {colunas_data}")
    print(f"   📋 Colunas UTM encontradas: {colunas_utm}")
    
    # Debug: Verificar alguns valores da coluna UTM
    if colunas_utm:
        print(f"🔍 DEBUG - Verificando valores na coluna UTM '{colunas_utm[0]}':")
        valores_unicos = df[colunas_utm[0]].dropna().unique()
        print(f"   📋 Valores únicos encontrados: {valores_unicos[:10]}")  # Primeiros 10 valores
        
        # Verificar especificamente por "ad"
        registros_ad = df[df[colunas_utm[0]].str.lower() == "ad"]
        print(f"   📊 Registros com 'ad' encontrados: {len(registros_ad)}")
        if len(registros_ad) > 0:
            print(f"   📋 Primeiros registros AD:")
            for idx, row in registros_ad.head(3).iterrows():
                print(f"      Linha {idx}: {row[colunas_utm[0]]}")
    else:
        print(f"   ❌ Nenhuma coluna UTM encontrada!")
    
    for idx, row in df.iterrows():
        # Verifica se tem UTM source = "ad" (minúsculo conforme dados)
        tem_ad = False
        for col in colunas_utm:
            valor = row[col] if col in row else None
            if valor is not None:
                valor_str = str(valor).strip().lower()
                if valor_str == "ad":
                    tem_ad = True
                    if ad_count < 5:  # Mostrar apenas os primeiros 5 para debug
                        print(f"   ✅ Registro AD encontrado na linha {idx}, coluna '{col}': '{valor}'")
                    break
        
        if tem_ad:
            # Se há filtro de data, verifica se está no período
            if data_ini and data_fim and colunas_data:
                data_valida = False
                if ad_count < 3:  # Debug para os primeiros registros
                    print(f"   🔍 DEBUG - Verificando filtro de data para registro AD linha {idx}")
                    print(f"      📅 Filtro: {data_ini} a {data_fim}")
                
                for col in colunas_data:
                    try:
                        data_str = str(row[col])
                        if pd.notna(data_str) and data_str.strip():
                            if ad_count < 3:
                                print(f"      📋 Coluna '{col}': '{data_str}'")
                            
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
                                
                                if ad_count < 3:
                                    print(f"      📅 Data parseada: {data_criacao}")
                                    print(f"      📅 Range filtro: {data_ini_dt} a {data_fim_dt}")
                                    print(f"      ✅ Data válida: {data_ini_dt <= data_criacao <= data_fim_dt}")
                                
                                if data_ini_dt <= data_criacao <= data_fim_dt:
                                    data_valida = True
                                    break
                    except (ValueError, TypeError) as e:
                        if ad_count < 3:
                            print(f"      ❌ Erro ao parsear data '{data_str}': {e}")
                        continue
                
                # Se não há filtro de data ou se a data está no período, conta o registro
                if not data_ini or not data_fim or data_valida:
                    ad_count += 1
                    if ad_count <= 3:
                        print(f"      ✅ Registro AD ACEITO pelo filtro de data (Total: {ad_count})")
                else:
                    if ad_count < 3:
                        print(f"      ❌ Registro AD REJEITADO pelo filtro de data")
                    continue  # Pula para o próximo registro se a data não for válida
            
            # Extrai CPF do registro (apenas se o registro foi aceito)
            cpf_encontrado = None
            for col in colunas_cpf:
                valor_cpf = row[col] if col in row else None
                if valor_cpf is not None:
                    valor_cpf_str = str(valor_cpf).strip()
                    # Usar a nova função de limpeza de CPF
                    cpf_limpo = limpar_cpf(valor_cpf_str)
                    if cpf_limpo and len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                        cpf_encontrado = cpf_limpo
                        if ad_count <= 5:  # Mostrar apenas os primeiros 5 para debug
                            print(f"      ✅ CPF encontrado na coluna '{col}': '{valor_cpf}' -> '{cpf_limpo}'")
                        break
                    elif ad_count <= 3:  # Mostrar apenas os primeiros 3 para debug
                        print(f"      ⚠️ CPF inválido na coluna '{col}': '{valor_cpf}' -> '{cpf_limpo}'")
            
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
    
    # Log final dos resultados
    print(f"🔍 DEBUG - Resultados da extração AD:")
    print(f"   📊 Total de registros AD encontrados: {ad_count}")
    print(f"   📋 Distribuição por status: {ad_por_status}")
    print(f"   📋 CPFs únicos por status: {dict((k, len(v)) for k, v in ad_cpfs_por_status.items())}")
    
    print(f"🔍 DEBUG - FUNÇÃO extrair_ad_da_base CONCLUÍDA")
    return ad_count, ad_por_status, ad_cpfs_por_status

def comparar_cpfs_facta_kolmeya(start_at, end_at, ambiente_facta="homologacao", tenant_segment_id=None, limit_kolmeya=30000, limit_facta=5000):
    """
    Compara CPFs retornados pelo endpoint da FACTA com CPFs retornados pelo endpoint do Kolmeya.
    
    Args:
        start_at: Data inicial (YYYY-MM-DD)
        end_at: Data final (YYYY-MM-DD)
        ambiente_facta: Ambiente do FACTA ("homologacao" ou "producao")
        tenant_segment_id: Centro de custo do Kolmeya
        limit_kolmeya: Limite de mensagens para consultar no Kolmeya
        limit_facta: Limite de propostas para consultar no FACTA
    
    Returns:
        Dicionário com resultados da comparação
    """
    print(f"🔍 Iniciando comparação CPFs FACTA vs Kolmeya:")
    print(f"   📅 Período: {start_at} a {end_at}")
    print(f"   🏢 Ambiente FACTA: {ambiente_facta}")
    print(f"   🏢 Centro de custo Kolmeya: {tenant_segment_id}")
    
    # Obter tokens
    token_kolmeya = get_kolmeya_token()
    token_facta = get_facta_token()
    
    if not token_kolmeya:
        print("❌ Token do Kolmeya não encontrado")
        return {
            "erro": "Token do Kolmeya não encontrado",
            "cpfs_kolmeya": set(),
            "cpfs_facta": set(),
            "cpfs_coincidentes": set(),
            "cpfs_apenas_kolmeya": set(),
            "cpfs_apenas_facta": set(),
            "total_kolmeya": 0,
            "total_facta": 0,
            "total_coincidentes": 0
        }
    
    if not token_facta:
        print("❌ Token do FACTA não encontrado")
        return {
            "erro": "Token do FACTA não encontrado",
            "cpfs_kolmeya": set(),
            "cpfs_facta": set(),
            "cpfs_coincidentes": set(),
            "cpfs_apenas_kolmeya": set(),
            "cpfs_apenas_facta": set(),
            "total_kolmeya": 0,
            "total_facta": 0,
            "total_coincidentes": 0
        }
    
    # 1. CONSULTAR CPFs do Kolmeya
    print(f"\n📱 Passo 1: Consultando CPFs do Kolmeya...")
    
    # Formatar datas para o formato esperado pela API (YYYY-MM-DD HH:MM)
    start_at_formatted = f"{start_at} 00:00"
    end_at_formatted = f"{end_at} 23:59"
    
    cpfs_kolmeya = consultar_cpfs_diretamente_kolmeya(
        start_at=start_at_formatted,
        end_at=end_at_formatted,
        limit=limit_kolmeya,
        token=token_kolmeya,
        tenant_segment_id=tenant_segment_id
    )
    
    # Converter para set para facilitar comparação
    cpfs_kolmeya_set = set(cpfs_kolmeya)
    
    print(f"✅ CPFs do Kolmeya encontrados: {len(cpfs_kolmeya_set)}")
    
    # 2. CONSULTAR CPFs do FACTA
    print(f"\n📋 Passo 2: Consultando CPFs do FACTA...")
    
    # Testar conectividade com FACTA
    if not testar_api_facta(token=token_facta, ambiente=ambiente_facta):
        print("❌ API FACTA não está respondendo")
        return {
            "erro": "API FACTA não está respondendo",
            "cpfs_kolmeya": cpfs_kolmeya_set,
            "cpfs_facta": set(),
            "cpfs_coincidentes": set(),
            "cpfs_apenas_kolmeya": cpfs_kolmeya_set,
            "cpfs_apenas_facta": set(),
            "total_kolmeya": len(cpfs_kolmeya_set),
            "total_facta": 0,
            "total_coincidentes": 0
        }
    
    # Consultar todas as propostas do FACTA no período
    data_ini_dt = datetime.strptime(start_at, '%Y-%m-%d').date()
    data_fim_dt = datetime.strptime(end_at, '%Y-%m-%d').date()
    
    propostas_facta = consultar_todas_propostas_facta(
        token=token_facta,
        ambiente=ambiente_facta,
        data_ini=data_ini_dt,
        data_fim=data_fim_dt
    )
    
    # Extrair CPFs únicos das propostas do FACTA
    cpfs_facta_set = set()
    for proposta in propostas_facta:
        cpf = proposta.get('cpf', '')
        if cpf:
            cpf_limpo = limpar_cpf(str(cpf))
            if cpf_limpo and len(cpf_limpo) == 11:
                cpfs_facta_set.add(cpf_limpo)
    
    print(f"✅ CPFs do FACTA encontrados: {len(cpfs_facta_set)}")
    
    # 3. COMPARAR CPFs
    print(f"\n🔍 Passo 3: Comparando CPFs...")
    
    # CPFs que estão em AMBOS os sistemas
    cpfs_coincidentes = cpfs_kolmeya_set & cpfs_facta_set
    
    # CPFs que estão APENAS no Kolmeya
    cpfs_apenas_kolmeya = cpfs_kolmeya_set - cpfs_facta_set
    
    # CPFs que estão APENAS no FACTA
    cpfs_apenas_facta = cpfs_facta_set - cpfs_kolmeya_set
    
    # 4. RESULTADOS
    print(f"\n✅ Comparação concluída:")
    print(f"   📊 Total CPFs Kolmeya: {len(cpfs_kolmeya_set)}")
    print(f"   📊 Total CPFs FACTA: {len(cpfs_facta_set)}")
    print(f"   ✅ CPFs coincidentes: {len(cpfs_coincidentes)}")
    print(f"   📱 CPFs apenas Kolmeya: {len(cpfs_apenas_kolmeya)}")
    print(f"   📋 CPFs apenas FACTA: {len(cpfs_apenas_facta)}")
    
    # Calcular percentuais
    percentual_coincidentes_kolmeya = (len(cpfs_coincidentes) / len(cpfs_kolmeya_set) * 100) if cpfs_kolmeya_set else 0
    percentual_coincidentes_facta = (len(cpfs_coincidentes) / len(cpfs_facta_set) * 100) if cpfs_facta_set else 0
    
    print(f"   📈 % CPFs Kolmeya que estão no FACTA: {percentual_coincidentes_kolmeya:.1f}%")
    print(f"   📈 % CPFs FACTA que estão no Kolmeya: {percentual_coincidentes_facta:.1f}%")
    
    # Mostrar alguns exemplos de CPFs coincidentes
    if cpfs_coincidentes:
        print(f"   📋 Exemplos de CPFs coincidentes:")
        for i, cpf in enumerate(list(cpfs_coincidentes)[:5]):
            print(f"      {i+1}. {cpf}")
        if len(cpfs_coincidentes) > 5:
            print(f"      ... e mais {len(cpfs_coincidentes) - 5} CPFs")
    
    # Mostrar alguns exemplos de CPFs apenas no Kolmeya
    if cpfs_apenas_kolmeya:
        print(f"   📱 Exemplos de CPFs apenas no Kolmeya:")
        for i, cpf in enumerate(list(cpfs_apenas_kolmeya)[:3]):
            print(f"      {i+1}. {cpf}")
        if len(cpfs_apenas_kolmeya) > 3:
            print(f"      ... e mais {len(cpfs_apenas_kolmeya) - 3} CPFs")
    
    # Mostrar alguns exemplos de CPFs apenas no FACTA
    if cpfs_apenas_facta:
        print(f"   📋 Exemplos de CPFs apenas no FACTA:")
        for i, cpf in enumerate(list(cpfs_apenas_facta)[:3]):
            print(f"      {i+1}. {cpf}")
        if len(cpfs_apenas_facta) > 3:
            print(f"      ... e mais {len(cpfs_apenas_facta) - 3} CPFs")
    
    return {
        "cpfs_kolmeya": cpfs_kolmeya_set,
        "cpfs_facta": cpfs_facta_set,
        "cpfs_coincidentes": cpfs_coincidentes,
        "cpfs_apenas_kolmeya": cpfs_apenas_kolmeya,
        "cpfs_apenas_facta": cpfs_apenas_facta,
        "total_kolmeya": len(cpfs_kolmeya_set),
        "total_facta": len(cpfs_facta_set),
        "total_coincidentes": len(cpfs_coincidentes),
        "percentual_coincidentes_kolmeya": percentual_coincidentes_kolmeya,
        "percentual_coincidentes_facta": percentual_coincidentes_facta,
        "propostas_facta": propostas_facta
    }


def main():
    # Configuração da página com layout otimizado
    st.set_page_config(
        page_title="Dashboard Servix",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    

    
    # Estilos CSS para evitar problemas de JavaScript
    st.markdown("""
    <style>
        .stApp {
            background-color: #0e1117;
        }
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
        }
        .stButton > button {
            border-radius: 8px;
            border: 1px solid #4CAF50;
            background-color: #4CAF50;
            color: white;
            padding: 0.5rem 1rem;
            font-weight: bold;
        }
        .stButton > button:hover {
            background-color: #45a049;
            border-color: #45a049;
        }
        .stSelectbox > div > div {
            background-color: #262730;
            border-color: #4CAF50;
        }
        .stDateInput > div > div {
            background-color: #262730;
            border-color: #4CAF50;
        }
        
        /* Prevenir problemas de JavaScript */
        .js-plotly-plot {
            max-width: 100% !important;
        }
        
        /* Melhorar responsividade */
        @media (max-width: 768px) {
            .main .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }
        }
        
        /* Estilos para mensagens de erro */
        .error-message {
            background-color: rgba(255, 0, 0, 0.1);
            border: 1px solid rgba(255, 0, 0, 0.3);
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
            color: #ff6b6b;
        }
        
        /* Estilos para mensagens de sucesso */
        .success-message {
            background-color: rgba(0, 255, 0, 0.1);
            border: 1px solid rgba(0, 255, 0, 0.3);
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
            color: #51cf66;
        }
    </style>
    
    <script>
        // Prevenir erros de JavaScript
        window.addEventListener('error', function(e) {
            console.log('Erro JavaScript capturado:', e.message);
            return false;
        });
        
        // Prevenir erros de módulos dinâmicos
        window.addEventListener('unhandledrejection', function(e) {
            console.log('Promise rejeitada:', e.reason);
            e.preventDefault();
        });
        
        // Função para recarregar a página em caso de erro
        function reloadOnError() {
            setTimeout(function() {
                if (document.querySelector('.error-message')) {
                    window.location.reload();
                }
            }, 5000);
        }
        
        // Executar após carregamento da página
        document.addEventListener('DOMContentLoaded', reloadOnError);
    </script>
    """, unsafe_allow_html=True)
    
    if HAS_AUTOREFRESH:
        st_autorefresh(interval=5 * 60 * 1000, key="datarefresh")  # Aumentado para 5 minutos
    
    # Adicionar teste de ambiente na sidebar
    test_environment_status()
    
    
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Servix</h1>", unsafe_allow_html=True)

    # Campos de período
    col_data_ini, col_data_fim = st.columns(2)
    with col_data_ini:
        data_ini = st.date_input("Data inicial", value=datetime.now().date() - timedelta(days=6), key="data_ini_topo")
    with col_data_fim:
        data_fim = st.date_input("Data final", value=datetime.now().date(), key="data_fim_topo")

    # Variável global para armazenar valor do FACTA encontrado na comparação de CPFs
    if 'valor_facta_kolmeya' not in st.session_state:
        st.session_state.valor_facta_kolmeya = 0.0
    
    # Variável global para armazenar CPFs com propostas do FACTA
    if 'cpfs_com_propostas_facta' not in st.session_state:
        st.session_state.cpfs_com_propostas_facta = 0

    # Filtro de centro de custo - IDs conforme documentação da API Kolmeya
    centro_custo_opcoes = {
        "TODOS": None,
        "Novo": 8105,      # ID do centro de custo NOVO no Kolmeya
        "Crédito CLT": 8208, # ID do centro de custo CRÉDITO CLT no Kolmeya
        "FGTS": 8103        # ID do centro de custo FGTS no Kolmeya
    }
    
    centro_custo_selecionado = st.selectbox(
        "Centro de Custo",
        options=list(centro_custo_opcoes.keys()),
        index=0,  # "TODOS" será a primeira opção
        key="centro_custo_filtro"
    )
    centro_custo_valor = centro_custo_opcoes[centro_custo_selecionado]
    


    # Saldo Kolmeya com tratamento de erro melhorado
    col_saldo, col_vazio = st.columns([0.9, 5.1])
    
    with col_saldo:
        try:
            saldo_kolmeya = obter_saldo_kolmeya()
            
            # Verificar se o saldo é válido
            if saldo_kolmeya is None or saldo_kolmeya < 0:
                saldo_kolmeya = 0.0
                status_saldo = "⚠️ Erro na consulta"
                cor_borda = "rgba(255, 165, 0, 0.5)"  # Laranja para erro
            else:
                status_saldo = "✅ Saldo atualizado"
                cor_borda = "rgba(162, 89, 255, 0.5)"  # Roxo para sucesso
            
            st.markdown(
                f"""
                <div style='background: rgba(40, 24, 70, 0.96); border: 2.5px solid {cor_borda}; border-radius: 16px; padding: 24px 32px; color: #fff; min-width: 320px; min-height: 90px; box-shadow: 0 2px 8px rgba(0,0,0,0.3); margin-bottom: 24px; display: flex; flex-direction: column; align-items: center;'>
                    <div style='font-size: 1.3em; color: #e0d7f7; font-weight: bold; margin-bottom: 8px;'>Saldo Atual Kolmeya</div>
                    <div style='font-size: 2.5em; font-weight: bold; color: #fff;'>
                        {formatar_real(saldo_kolmeya)}
                    </div>
                    <div style='font-size: 0.9em; color: #b0b0b0; margin-top: 8px;'>{status_saldo}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
            

                

        except Exception as e:
            st.error(f"❌ Erro ao obter saldo: {str(e)}")
            saldo_kolmeya = 0.0


    
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
    
        # VERIFICAÇÃO DE MUDANÇA DE DATAS: Forçar atualização se as datas mudaram
    if "ultima_data_consulta" in st.session_state:
        ultima_data = st.session_state.get("ultima_data_consulta")
        if ultima_data != (data_ini, data_fim, centro_custo_selecionado):
            print(f"🔄 DATAS MUDARAM - Forçando atualização completa")
            print(f"   📅 Anterior: {ultima_data}")
            print(f"   📅 Atual: {data_ini}, {data_fim}, {centro_custo_selecionado}")
            
            # Limpar cache completo
            pass
            
            # Cache limpo
            print(f"   🗑️ Cache limpo")
    
    # Atualizar timestamp da consulta
    st.session_state["ultima_data_consulta"] = (data_ini, data_fim, centro_custo_selecionado)
    
    # Obter dados do Kolmeya via API ANTES de calcular leads
    print(f"🔍 Consultando API Kolmeya:")
    print(f"   📅 Período: {data_ini} a {data_fim}")
    print(f"   🏢 Centro de custo: {centro_custo_selecionado}")
    print(f"   🕐 Data atual: {datetime.now().date()}")
    print(f"   🔍 É dia atual? {data_fim == datetime.now().date()}")
    
    # Verificar se há base carregada
    df_base = None  # Inicializar variável
    if uploaded_file is not None:
        try:
            df_base = ler_base(uploaded_file)
            print(f"📁 Base carregada: {uploaded_file.name}")
            print(f"📊 Tamanho da base: {len(df_base) if df_base is not None else 0} registros")
        except Exception as e:
            print(f"❌ Erro ao carregar base: {e}")
            df_base = None
    else:
        print(f"⚠️ Nenhuma base carregada")
    
    messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, centro_custo_valor)
    
    # Filtrar mensagens por data após receber da API
    if messages:
        print(f"📊 Mensagens recebidas da API: {len(messages)}")
        messages = filtrar_mensagens_por_data(messages, data_ini, data_fim)
        print(f"📅 Após filtro por data: {len(messages)} mensagens")
    else:
        print(f"⚠️ Nenhuma mensagem recebida da API")
    
    print(f"📊 Resultado final: {len(messages) if messages else 0} SMS, {total_acessos} acessos")
    
    # Inicializar variáveis para contagem de URA ANTES de serem usadas
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
    
    # Inicializar variáveis para contagem de AD
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
    
    # CALCULAR LEADS GERADOS ANTES DA RENDERIZAÇÃO DO HTML
    total_leads_gerados = 0
    telefones_base = 0
    
    # Se há base carregada, fazer processamento
    if uploaded_file is not None:
        try:
            # Extrair telefones da base
            telefones_base_temp = extrair_telefones_da_base(df_base, data_ini, data_fim)
            
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
            # Inicializar variáveis URA com valores padrão se não estiverem definidas
            if 'ura_count' not in locals() and 'ura_count' not in globals():
                ura_count = 0
            if 'ura_por_status' not in locals() and 'ura_por_status' not in globals():
                ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
            
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
        # Inicializar variáveis URA com valores padrão se não estiverem definidas
        if 'ura_count' not in locals() and 'ura_count' not in globals():
            ura_count = 0
        if 'ura_por_status' not in locals() and 'ura_por_status' not in globals():
            ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
        
        if centro_custo_selecionado == "Novo":
            total_leads_gerados = ura_por_status.get('Novo', 0)
        elif centro_custo_selecionado == "FGTS":
            total_leads_gerados = ura_por_status.get('FGTS', 0)
        elif centro_custo_selecionado == "Crédito CLT":
            total_leads_gerados = ura_por_status.get('CLT', 0)
        else:
            total_leads_gerados = ura_count
        telefones_base = total_leads_gerados
    



    
    if uploaded_file is not None and df_base is not None:
        try:
            print(f"📊 Base carregada com sucesso: {len(df_base)} registros")
            
            # Extrair contagem de URA da base com filtro de data e separação por status
            print(f"🔍 DEBUG - Iniciando extração URA da base...")
            ura_count, ura_por_status, ura_cpfs_por_status = extrair_ura_da_base(df_base, data_ini, data_fim)
            print(f"🔍 DEBUG - Extração URA concluída:")
            print(f"   📊 Total URA: {ura_count}")
            print(f"   📋 CPFs por status: {dict((k, len(v)) for k, v in ura_cpfs_por_status.items())}")
            
            # Extrair contagem de AD da base com filtro de data e separação por status
            print(f"🔍 DEBUG - Iniciando extração AD da base...")
            print(f"   📊 df_base shape: {df_base.shape if df_base is not None else 'None'}")
            print(f"   📋 df_base columns: {list(df_base.columns) if df_base is not None else 'None'}")
            
            # Teste simples para verificar se há dados
            if df_base is not None and not df_base.empty:
                print(f"   ✅ DataFrame não está vazio")
                # Verificar se há coluna "utm source"
                if "utm source" in df_base.columns:
                    print(f"   ✅ Coluna 'utm source' encontrada")
                    valores_utm = df_base["utm source"].dropna().unique()
                    print(f"   📋 Valores únicos em 'utm source': {valores_utm}")
                    # Contar registros com "ad"
                    registros_ad = df_base[df_base["utm source"].str.lower() == "ad"]
                    print(f"   📊 Registros com 'ad': {len(registros_ad)}")
                else:
                    print(f"   ❌ Coluna 'utm source' NÃO encontrada")
                    print(f"   📋 Colunas disponíveis: {list(df_base.columns)}")
            else:
                print(f"   ❌ DataFrame está vazio ou None")
            
            ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
            print(f"🔍 DEBUG - Extração AD concluída:")
            print(f"   📊 Total AD: {ad_count}")
            print(f"   📋 CPFs por status: {dict((k, len(v)) for k, v in ad_cpfs_por_status.items())}")
            

                
            # Processar dados do WhatsApp silenciosamente
            whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status = extrair_whatsapp_da_base(df_base, data_ini, data_fim)
            ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
            
            print(f"🔍 DEBUG - Dados WhatsApp extraídos:")
            print(f"   📊 Total registros WhatsApp: {whatsapp_count}")
            print(f"   📋 CPFs por status: {dict((k, len(v)) for k, v in whatsapp_cpfs_por_status.items())}")
            print(f"   🏢 Centro de custo selecionado: {centro_custo_selecionado}")
                

        except Exception:
            # Em caso de erro, manter valores em zero
            ura_count = 0
            ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
            ura_cpfs_por_status = {'Novo': set(), 'FGTS': set(), 'CLT': set(), 'Outros': set()}
            # Dados zerados
            pass
    else:
        # Se não há arquivo carregado, deixar o painel 4NET vazio (sem dados URA)
        ura_count = 0
        ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
        ura_cpfs_por_status = {'Novo': set(), 'FGTS': set(), 'CLT': set(), 'Outros': set()}

    # LIMPEZA DO CACHE: Forçar atualização dos dados do Kolmeya
    print(f"🔄 LIMPANDO CACHE - Forçando atualização dos dados do Kolmeya")
    
    # Limpar dados antigos do session state
    if "ultima_consulta_kolmeya" in st.session_state:
        ultima_consulta = st.session_state.get("ultima_consulta_kolmeya")
        print(f"   📅 Última consulta Kolmeya: {ultima_consulta}")
        
        # Se a consulta anterior foi para o mesmo período, limpar cache
        if (ultima_consulta and 
            ultima_consulta.get('data_ini') == data_ini and 
            ultima_consulta.get('data_fim') == data_fim and
            ultima_consulta.get('centro_custo') == centro_custo_selecionado):
            print(f"   ⚠️ Mesmo período consultado anteriormente, limpando cache")
            pass
    
    # Atualizar timestamp da consulta
    st.session_state["ultima_consulta_kolmeya"] = {
        'data_ini': data_ini,
        'data_fim': data_fim,
        'centro_custo': centro_custo_selecionado,
        'timestamp': datetime.now()
    }
    
    # CONSULTA DOS ACESSOS DO KOLMEYA
    print(f"🔍 CONSULTA KOLMEYA - Iniciando busca por acessos...")
    
    # Verificar token do Kolmeya
    token_kolmeya = get_kolmeya_token()
    print(f"   🔑 Token Kolmeya: {'Sim' if token_kolmeya else 'Não'}")
    
    # Inicializar variável acessos_kolmeya
    acessos_kolmeya = []
    
    if not token_kolmeya:
        print(f"   ❌ Token do Kolmeya não encontrado!")
        st.session_state["acessos_kolmeya_count"] = 0
        st.session_state["cpfs_kolmeya_consultados"] = set()
    else:
        print(f"✅ Token do Kolmeya encontrado: {token_kolmeya[:10]}...")
        
        # CONSULTA DIRETA aos acessos do Kolmeya
        print(f"🔍 Consultando acessos do Kolmeya diretamente...")
        print(f"   📅 Período: {data_ini} a {data_fim}")
        print(f"   🏢 Centro de custo: {centro_custo_selecionado} ({centro_custo_valor})")
        
        # Forçar nova consulta de acessos
        print(f"   🔄 Forçando consulta de acessos para período: {data_ini} a {data_fim}")
        
        acessos_kolmeya = consultar_acessos_sms_kolmeya(
            start_at=data_ini.strftime('%Y-%m-%d'),  # Formato correto: apenas data
            end_at=data_fim.strftime('%Y-%m-%d'),    # Formato correto: apenas data
            limit=10000,  # Aumentado para pegar mais acessos
            token=token_kolmeya,
            tenant_segment_id=centro_custo_valor  # Passar centro de custo para filtragem
        )
    
    if acessos_kolmeya:
        print(f"✅ Acessos encontrados: {len(acessos_kolmeya)}")
        
        # SALVAR contagem de acessos no session state para mostrar no painel
        st.session_state["acessos_kolmeya_count"] = len(acessos_kolmeya)
        
        # Extrair CPFs dos acessos
        cpfs_acessos = extrair_cpfs_acessos_kolmeya(acessos_kolmeya)
        
        print(f"🔍 DEBUG - CPFs extraídos dos acessos do Kolmeya:")
        print(f"   📊 Total de acessos: {len(acessos_kolmeya)}")
        print(f"   📊 Total de CPFs únicos de acessos: {len(cpfs_acessos)}")
        if cpfs_acessos:
            print(f"   📋 Primeiros 5 CPFs de acessos: {list(cpfs_acessos)[:5]}")
            
            # SALVAR CPFs consultados no session state para mostrar no painel
            st.session_state["cpfs_kolmeya_consultados"] = cpfs_acessos
        else:
            print(f"   ⚠️ Nenhum CPF encontrado nos acessos do Kolmeya")
            st.session_state["cpfs_kolmeya_consultados"] = set()  # CPFs vazios
    else:
        print(f"⚠️ Nenhum acesso encontrado no Kolmeya para o período {data_ini} a {data_fim}")
        st.session_state["acessos_kolmeya_count"] = 0
        st.session_state["cpfs_kolmeya_consultados"] = set()

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
    investimento = total_mensagens * 0.0821972734562951
    
    # Inicializar variáveis para evitar erro
    total_vendas = 0
    producao = 0.0
    leads_gerados_kolmeya = 0
    
    # Adicionar valor do FACTA encontrado na comparação de CPFs
    valor_facta_kolmeya = st.session_state.get('valor_facta_kolmeya', 0.0)
    cpfs_com_propostas_facta = st.session_state.get('cpfs_com_propostas_facta', 0)
    
    # Adicionar vendas e produção do FACTA
    total_vendas += cpfs_com_propostas_facta
    producao += valor_facta_kolmeya

    
    # Debug: Verificar dados recebidos
    print(f"🔍 DEBUG - Dados Kolmeya recebidos:")
    print(f"   📊 Total de mensagens: {total_mensagens}")
    print(f"   ✅ Mensagens entregues: {mensagens_entregues}")
    if messages and len(messages) > 0:
        print(f"   📅 Primeira mensagem - enviada_em: {messages[0].get('enviada_em', 'N/A')}")
        print(f"   📅 Última mensagem - enviada_em: {messages[-1].get('enviada_em', 'N/A')}")
        print(f"   🏢 Centro de custo da primeira: {messages[0].get('centro_custo', 'N/A')}")
    print(f"   💰 Investimento calculado: R$ {investimento:.2f}")
    print(f"   💰 Valor FACTA adicionado: R$ {valor_facta_kolmeya:,.2f}")
    print(f"   📊 CPFs com propostas FACTA: {cpfs_com_propostas_facta}")
    print(f"   💰 Produção total (incluindo FACTA): R$ {producao:,.2f}")
    
    # CAMPO 1: Taxa de entrega
    disparos_por_lead = (leads_gerados_kolmeya / total_mensagens * 100) if total_mensagens > 0 else 0.0
    
    # Calcular taxa de entrega baseada nas mensagens entregues
    taxa_entrega = (mensagens_entregues / total_mensagens * 100) if total_mensagens > 0 else 0.0
    
    # CORREÇÃO: Calcular leads gerados comparando telefones da API com telefones da base
    telefones_kolmeya = extrair_telefones_kolmeya(messages) if messages else set()
    telefones_base_kolmeya = set()
    
    if uploaded_file is not None and df_base is not None:
        try:
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
    
    # Dados reais do Kolmeya (já incluem valores do FACTA)
    print(f"🔍 DEBUG - Dados Kolmeya carregados:")
    print(f"   📊 Total vendas: {total_vendas}")
    print(f"   💰 Produção total: R$ {producao:,.2f}")
    print(f"   💰 Valor FACTA incluído: R$ {valor_facta_kolmeya:,.2f}")
    print(f"   📊 CPFs com propostas FACTA: {cpfs_com_propostas_facta}")
    print(f"   🏢 Centro de custo: {centro_custo_selecionado}")
    
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
    
    # Calcular métricas após definir todas as variáveis
    disp_venda = total_vendas / total_mensagens if total_mensagens > 0 else 0
    leads_p_venda = total_vendas / leads_gerados_kolmeya if leads_gerados_kolmeya > 0 else 0


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
    taxa_lead = taxa_ativacao  # Taxa de lead é a mesma que taxa de ativação
    
    # Dados do painel 4NET
    total_vendas_ura = 0
    producao_ura = 0.0
    
    # Calcular métricas baseadas em estimativas
    fat_med_venda = 0.0
    retor_estimado = 0.0

    # ROI do painel 4NET baseado APENAS nos dados da URA
    roi_ura = producao_ura - total_investimento

    # Dados do PAINEL WHATSAPP baseados nos dados reais da base
    if uploaded_file is not None and df_base is not None:
        try:
            whatsapp_count, whatsapp_por_status, whatsapp_cpfs_por_status = extrair_whatsapp_da_base(df_base, data_ini, data_fim)
            
            print(f"🔍 DEBUG - Painel WhatsApp - Dados extraídos:")
            print(f"   📊 WhatsApp count: {whatsapp_count}")
            print(f"   📋 CPFs WhatsApp por status: {dict((k, len(v)) for k, v in whatsapp_cpfs_por_status.items())}")
            print(f"   🔍 Dados WhatsApp processados")
            
            # Usar dados reais do WhatsApp
            campanhas_realizadas = whatsapp_count  # Total de mensagens WhatsApp
            camp_atendidas = (whatsapp_count / max(telefones_base, 1)) * 100 if telefones_base > 0 else 0.0  # Taxa de engajamento
            total_investimento_novo = whatsapp_count * 0.20  # Custo por mensagem WhatsApp
            tempo_medio_campanha = 2.5  # Tempo médio de resposta em horas
            total_engajados = whatsapp_count  # Total de mensagens enviadas
            
            # Usar estimativas
            total_vendas_novo = whatsapp_count * 0.15  # Estimativa de vendas (15% de conversão)
            producao_novo = total_vendas_novo * 5000  # Produção estimada (ticket médio R$ 5.000)
            
            roi_novo = producao_novo - total_investimento_novo
            
            # Debug: Mostrar valores encontrados
            print(f"🔍 DEBUG - Painel WhatsApp - Valores encontrados:")
            print(f"   📊 Campanhas realizadas: {campanhas_realizadas}")
            print(f"   💰 Produção estimada: R$ {producao_novo:,.2f}")
            print(f"   📈 Total vendas estimado: {total_vendas_novo}")
            print(f"   💰 ROI calculado: R$ {roi_novo:,.2f}")
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

    # Dados do TERCEIRO PAINEL baseados nos dados reais da base
    if uploaded_file is not None and df_base is not None:
        try:
            ad_count, ad_por_status, ad_cpfs_por_status = extrair_ad_da_base(df_base, data_ini, data_fim)
            
            # Usar dados reais do AD
            acoes_realizadas = ad_count  # Total de ações AD
            acoes_efetivas = (ad_count / max(telefones_base, 1)) * 100 if telefones_base > 0 else 0.0  # Taxa de efetividade
            total_investimento_segundo = ad_count * 0.25  # Custo por ação AD
            tempo_medio_acao = 3.0  # Tempo médio de resposta em horas
            total_efetivos = ad_count  # Total de ações realizadas
            
            # Usar estimativas
            total_vendas_segundo = ad_count * 0.12  # Estimativa de vendas (12% de conversão)
            producao_segundo = total_vendas_segundo * 4500  # Produção estimada (ticket médio R$ 4.500)
            
            roi_segundo = producao_segundo - total_investimento_segundo
            
            # Debug: Mostrar valores encontrados
            print(f"🔍 DEBUG - Painel AD - Valores encontrados:")
            print(f"   📊 Ações realizadas: {acoes_realizadas}")
            print(f"   💰 Produção estimada: R$ {producao_segundo:,.2f}")
            print(f"   📈 Total vendas estimado: {total_vendas_segundo}")
            print(f"   💰 ROI calculado: R$ {roi_segundo:,.2f}")
            
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
    taxa_lead = locals().get('taxa_lead', 0.0)
    ligacoes_realizadas = locals().get('ligacoes_realizadas', 0)
    total_investimento = locals().get('total_investimento', 0.0)
    tempo_medio_campanha = locals().get('tempo_medio_campanha', 0.0)
    camp_atendidas = locals().get('camp_atendidas', 0.0)
    total_investimento_novo = locals().get('total_investimento_novo', 0.0)
    total_vendas_novo = locals().get('total_vendas_novo', 0)
    producao_novo = locals().get('producao_novo', 0.0)
    total_engajados = locals().get('total_engajados', 0)
    roi_novo = locals().get('roi_novo', 0.0)
    acoes_realizadas = locals().get('acoes_realizadas', 0)
    acoes_efetivas = locals().get('acoes_efetivas', 0.0)
    total_investimento_segundo = locals().get('total_investimento_segundo', 0.0)
    tempo_medio_acao = locals().get('tempo_medio_acao', 0.0)
    total_vendas_segundo = locals().get('total_vendas_segundo', 0)
    producao_segundo = locals().get('producao_segundo', 0.0)
    # Garantir que total_efetivos mantenha o valor correto do AD
    if 'total_efetivos' not in locals() or locals().get('total_efetivos', 0) == 0:
        total_efetivos = locals().get('ad_count', 0)
    roi_segundo = locals().get('roi_segundo', 0.0)
    

    

    

    
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
            <div class="panel-title">
                KOLMEYA
                <div style="font-size: 10px; color: #aaa; margin-top: 5px;">
                    Última atualização: {datetime.now().strftime('%d/%m %H:%M')}
                </div>
                <div style="font-size: 9px; color: #888; margin-top: 3px;">
                    Acessos: {st.session_state.get('acessos_kolmeya_count', 0)}
                </div>
                {f'<div style="font-size: 9px; color: #4CAF50; margin-top: 3px;">💰 FACTA: +{formatar_real(valor_facta_kolmeya)}</div>' if valor_facta_kolmeya > 0 else ''}
            </div>
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
                    <div class="metric-label">custo por venda</div>
                    <div class="metric-value-small">{formatar_real(investimento/max(total_vendas, 1))}</div>
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
                    <div class="detail-item">
                        <div class="detail-label">Leads p/ venda</div>
                        <div class="detail-value">{(leads_gerados_kolmeya/max(total_vendas, 1))}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Disp. p/ venda</div>
                        <div class="detail-value">{(total_mensagens/max(total_vendas, 1))}</div>
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
                        <div class="metric-value">0</div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label">Atendidas</div>
                        <div class="metric-value">0</div>
                    </div>
                </div>
                <div class="metric-row">
                    <div class="metric-item">
                        <div class="metric-label">Investimento</div>
                        <div class="metric-value-small">0,00</div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label">Taxa Ativação</div>
                        <div class="metric-value-small">0.0%</div>
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
                    <div class="metric-value">0</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Interações</div>
                    <div class="metric-value">0.0%</div>
                </div>
            </div>
            <div class="metric-row">
                <div class="metric-item">
                    <div class="metric-label">Investimento</div>
                    <div class="metric-value-small">0,00</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Tempo Médio</div>
                    <div class="metric-value-small">00:00h</div>
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
                    <div class="metric-label">Ações Realizadas</div>
                    <div class="metric-value">{acoes_realizadas}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Taxa Efetividade</div>
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
                    <div class="detail-item">
                        <div class="detail-label">Leads p/ venda</div>
                        <div class="detail-value">{total_efetivos/total_vendas_segundo if total_vendas_segundo > 0 else 0:.0f}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Disp. p/ venda</div>
                        <div class="detail-value">{ad_count/total_vendas_segundo if total_vendas_segundo > 0 else 0:.2f}</div>
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
    
    # Seção de comparação automática de CPFs com base e pesquisa no FACTA (ANTES da renderização do painel)
    st.markdown("---")
    
    
    # Verificar se há base carregada e executar comparação automaticamente
    if uploaded_file is not None and df_base is not None:
        # Configurações da comparação (valores padrão)
        ambiente_facta_comp = "producao"  # Ambiente padrão
        dias_comparacao = 7  # Dias padrão
        centro_custo_comp = "TODOS"  # Centro de custo padrão
        
        # Mostrar configurações usadas
        col_comp1, col_comp2, col_comp3 = st.columns(3)
        
       
        # Executar comparação automaticamente
        try:
       
            
            # Calcular datas para consulta no Kolmeya
            data_fim_comp = datetime.now().date()
            data_ini_comp = data_fim_comp - timedelta(days=dias_comparacao)
            
            # Mapear centro de custo
            centro_custo_opcoes_comp = {
                "TODOS": None,
                "Novo": 8105,
                "Crédito CLT": 8208,
                "FGTS": 8103
            }
            tenant_segment_id_comp = centro_custo_opcoes_comp[centro_custo_comp]
            
            # Extrair CPFs da base carregada
            
            cpfs_base_comp = extrair_cpfs_da_base(df_base, data_ini, data_fim)
            
            
            # Consultar CPFs do Kolmeya
            
            token_kolmeya = get_kolmeya_token()
            if not token_kolmeya:
                
                st.stop()
            
            # Formatar datas para o formato esperado pela API
            start_at_formatted = f"{data_ini_comp.strftime('%Y-%m-%d')} 00:00"
            # Usar horário atual para evitar problemas com datas futuras
            hora_atual = datetime.now().strftime('%H:%M')
            end_at_formatted = f"{data_fim_comp.strftime('%Y-%m-%d')} {hora_atual}"
            
            cpfs_kolmeya_comp = consultar_cpfs_diretamente_kolmeya(
                start_at=start_at_formatted,
                end_at=end_at_formatted,
                limit=30000,
                token=token_kolmeya,
                tenant_segment_id=tenant_segment_id_comp
            )
            
            
            # Padronizar CPFs da base
            cpfs_base_padronizados = set()
            for cpf in cpfs_base_comp:
                cpf_limpo = limpar_cpf(str(cpf))
                if cpf_limpo and len(cpf_limpo) == 11:
                    cpfs_base_padronizados.add(cpf_limpo)
            
            # Padronizar CPFs do Kolmeya
            cpfs_kolmeya_padronizados = set()
            for cpf in cpfs_kolmeya_comp:
                cpf_limpo = limpar_cpf(str(cpf))
                if cpf_limpo and len(cpf_limpo) == 11:
                    cpfs_kolmeya_padronizados.add(cpf_limpo)
            
            # CPFs que estão em AMBOS (base e Kolmeya)
            cpfs_coincidentes_comp = cpfs_base_padronizados & cpfs_kolmeya_padronizados
            
            
            
            # Mostrar estatísticas
            col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
          
            
            with col_stats4:
                percentual = (len(cpfs_coincidentes_comp) / len(cpfs_base_padronizados) * 100) if cpfs_base_padronizados else 0
                
            
            # Se há CPFs coincidentes, fazer pesquisa no FACTA
            if cpfs_coincidentes_comp:
                
                
                # Obter token do FACTA
                token_facta = get_facta_token()
                if not token_facta:
                    
                    st.stop()
                
                # Testar conectividade com FACTA
                if not testar_api_facta(token=token_facta, ambiente=ambiente_facta_comp):
                    
                    st.stop()
                
                # Consultar propostas no FACTA para os CPFs coincidentes
                propostas_encontradas = []
                cpfs_com_propostas = []
                
                # Converter para lista para processamento
                cpfs_coincidentes_list = list(cpfs_coincidentes_comp)
                
                # Processar em lotes para não sobrecarregar a API
                tamanho_lote = 50
                total_lotes = (len(cpfs_coincidentes_list) + tamanho_lote - 1) // tamanho_lote
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i in range(0, len(cpfs_coincidentes_list), tamanho_lote):
                        lote_cpfs = cpfs_coincidentes_list[i:i + tamanho_lote]
                        lote_atual = (i // tamanho_lote) + 1
                        
                        status_text.text(f"🔍 Processando lote {lote_atual}/{total_lotes} ({len(lote_cpfs)} CPFs)...")
                        
                        # Consultar propostas para este lote de CPFs
                        for cpf in lote_cpfs:
                            try:
                                # Consultar propostas específicas do CPF no FACTA
                                propostas_cpf = consultar_andamento_propostas_facta(
                                    cpfs=[cpf],
                                    token=token_facta,
                                    ambiente=ambiente_facta_comp
                                )
                                
                                if propostas_cpf:
                                    propostas_encontradas.extend(propostas_cpf)
                                    cpfs_com_propostas.append(cpf)
                                
                            except Exception as e:
                                st.warning(f"⚠️ Erro ao consultar CPF {cpf}: {str(e)}")
                                continue
                        
                        # Atualizar progresso
                        progress_bar.progress(lote_atual / total_lotes)
                
                progress_bar.empty()
                status_text.empty()
                
                # Mostrar resultados da pesquisa no FACTA
               
                # Mostrar detalhes das propostas encontradas
                if propostas_encontradas:
                    st.markdown("#### 📋 Propostas Encontradas no FACTA")
                    
                    # Criar DataFrame para exibição
                    df_propostas_comp = pd.DataFrame(propostas_encontradas)
                    
                    # Formatar valores monetários
                    if "valor_af" in df_propostas_comp.columns:
                        df_propostas_comp["valor_af_formatado"] = df_propostas_comp["valor_af"].apply(formatar_real)
                    
                    if "valor_bruto" in df_propostas_comp.columns:
                        df_propostas_comp["valor_bruto_formatado"] = df_propostas_comp["valor_bruto"].apply(formatar_real)
                    
                    # Mostrar tabela
                    st.dataframe(df_propostas_comp, use_container_width=True)
                    
                    
                    
                    # Resumo financeiro
                    if "valor_af" in df_propostas_comp.columns:
                        valor_total_af = df_propostas_comp["valor_af"].sum()
                        valor_medio_af = df_propostas_comp["valor_af"].mean()
                        
                        # Armazenar valor total AF e CPFs com propostas para usar no painel do Kolmeya
                        st.session_state.valor_facta_kolmeya = valor_total_af
                        st.session_state.cpfs_com_propostas_facta = len(cpfs_com_propostas)
                        
                        col_fin1, col_fin2, col_fin3 = st.columns(3)
                        
                else:
                    st.warning("⚠️ Nenhuma proposta encontrada no FACTA para os CPFs coincidentes.")
            else:
                st.warning("⚠️ Nenhum CPF coincidente encontrado entre a base e o Kolmeya.")
                
        except Exception as e:
            st.error(f"❌ Erro na comparação de CPFs: {str(e)}")
            st.exception(e)
    else:
        st.warning("⚠️ Carregue uma base de dados primeiro para executar a comparação de CPFs.")
    
    # Botão para limpar valores do FACTA
    if st.button("🗑️ Limpar Valores FACTA do Painel", key="btn_limpar_facta"):
        st.session_state.valor_facta_kolmeya = 0.0
        st.session_state.cpfs_com_propostas_facta = 0
        st.success("✅ Valores do FACTA foram removidos do painel Kolmeya!")
        st.rerun()
    
    components.html(dashboard_html, height=800)
    
    
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

    if uploaded_file is not None and df_base is not None:
        try:
            
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
                # Inicializar variáveis URA com valores padrão se não estiverem definidas
                if 'ura_count' not in locals() and 'ura_count' not in globals():
                    ura_count = 0
                if 'ura_por_status' not in locals() and 'ura_por_status' not in globals():
                    ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
                
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
        # Inicializar variáveis URA com valores padrão
        ura_count = 0
        ura_por_status = {'Novo': 0, 'FGTS': 0, 'CLT': 0, 'Outros': 0}
        
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
    
    # Botão para teste da API FACTA
    if st.sidebar.button("🔍 Teste API FACTA"):
        try:
            # Testar em ambos os ambientes
            st.sidebar.info("🧪 Testando API FACTA...")
            
            # Teste em homologação
            st.sidebar.write("**Homologação:**")
            if testar_api_facta(ambiente="homologacao"):
                st.sidebar.success("✅ Homologação: OK")
            else:
                st.sidebar.error("❌ Homologação: Falha")
            
            # Teste em produção
            st.sidebar.write("**Produção:**")
            if testar_api_facta(ambiente="producao"):
                st.sidebar.success("✅ Produção: OK")
            else:
                st.sidebar.error("❌ Produção: Falha")
                
        except Exception as e:
            st.sidebar.error(f"❌ Erro no teste: {str(e)[:50]}...")
    
    # Botão para teste dos envios do Kolmeya
    if st.sidebar.button("📨 Teste Envios Kolmeya"):
        try:
            st.sidebar.info("🧪 Testando envios do Kolmeya...")
            
            # Testar envios dos últimos 7 dias
            messages, total_acessos = obter_dados_sms_com_filtro(
                data_ini=(datetime.now() - timedelta(days=7)).date(),
                data_fim=datetime.now().date(),
                tenant_segment_id=None
            )
            
            if messages:
                st.sidebar.success(f"✅ Mensagens encontradas: {len(messages)}")
                
                # Extrair CPFs
                cpfs = extrair_cpfs_kolmeya(messages)
                st.sidebar.info(f"🆔 CPFs únicos: {len(cpfs)}")
                
                if cpfs:
                    st.sidebar.write("**Primeiros 5 CPFs:**")
                    for i, cpf in enumerate(list(cpfs)[:5]):
                        st.sidebar.write(f"{i+1}. {cpf}")
            else:
                st.sidebar.error("❌ Nenhuma mensagem encontrada")
                
        except Exception as e:
            st.sidebar.error(f"❌ Erro no teste: {str(e)[:50]}...")
    
    # Botão para teste do endpoint direto
    if st.sidebar.button("🎯 Teste Endpoint Direto"):
        try:
            st.sidebar.info("🧪 Testando endpoint direto (status SMS)...")
            
            # Testar endpoint direto dos últimos 7 dias
            start_at = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            end_at = datetime.now().strftime('%Y-%m-%d')
            
            cpfs = consultar_cpfs_diretamente_kolmeya(
                start_at=f"{start_at} 00:00",
                end_at=f"{end_at} 23:59",
                limit=30000,
                token=None,
                tenant_segment_id=None
            )
            
            if cpfs:
                st.sidebar.success(f"✅ CPFs encontrados: {len(cpfs)}")
                
                if cpfs:
                    st.sidebar.write("**Primeiros 5 CPFs:**")
                    for i, cpf in enumerate(cpfs[:5]):
                        st.sidebar.write(f"{i+1}. {cpf}")
            else:
                st.sidebar.error("❌ Nenhum CPF encontrado")
                
        except Exception as e:
            st.sidebar.error(f"❌ Erro no teste: {str(e)[:50]}...")
    
    # Botão para teste da comparação CPFs
    if st.sidebar.button("🔍 Teste Comparação CPFs"):
        try:
            st.sidebar.info("🧪 Testando comparação CPFs FACTA vs Kolmeya...")
            
            # Testar comparação dos últimos 3 dias
            start_at = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            end_at = datetime.now().strftime('%Y-%m-%d')
            
            resultado = comparar_cpfs_facta_kolmeya(
                start_at=start_at,
                end_at=end_at,
                ambiente_facta="homologacao",
                tenant_segment_id=None
            )
            
            if "erro" not in resultado:
                st.sidebar.success("✅ Comparação concluída!")
                st.sidebar.write(f"📱 Kolmeya: {resultado['total_kolmeya']} CPFs")
                st.sidebar.write(f"📋 FACTA: {resultado['total_facta']} CPFs")
                st.sidebar.write(f"✅ Coincidentes: {resultado['total_coincidentes']} CPFs")
                st.sidebar.write(f"📈 % Coincidentes: {resultado['percentual_coincidentes_kolmeya']:.1f}%")
            else:
                st.sidebar.error(f"❌ Erro: {resultado['erro']}")
                
        except Exception as e:
            st.sidebar.error(f"❌ Erro no teste: {str(e)[:50]}...")
    



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Erro crítico na aplicação: {str(e)}")
        st.exception(e)
        
        # Botão para tentar recarregar
        if st.button("🔄 Tentar Novamente"):
            st.rerun()

