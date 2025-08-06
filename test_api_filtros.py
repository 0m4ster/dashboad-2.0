import os
import sys
from datetime import datetime, timedelta

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do arquivo principal
from api_kolm import obter_dados_sms_com_filtro, consultar_status_sms_kolmeya

def test_filtros_api():
    """Testa diferentes cenários de filtragem da API"""
    
    # Definir período de teste (últimos 7 dias)
    data_fim = datetime.now().date()
    data_ini = data_fim - timedelta(days=7)
    
    print("=== TESTE DE FILTROS DA API ===")
    print(f"Período: {data_ini} até {data_fim}")
    
    # Teste 1: Sem filtro (TODOS)
    print("\n1. Teste SEM filtro (TODOS):")
    try:
        messages_todos, acessos_todos = obter_dados_sms_com_filtro(data_ini, data_fim, None)
        print(f"  - Mensagens retornadas: {len(messages_todos)}")
        print(f"  - Acessos retornados: {acessos_todos}")
        
        if messages_todos:
            primeira_msg = messages_todos[0]
            print(f"  - Campos da primeira mensagem: {list(primeira_msg.keys())}")
            if 'tenant_segment_id' in primeira_msg:
                print(f"  - tenant_segment_id: {primeira_msg['tenant_segment_id']}")
    except Exception as e:
        print(f"  - Erro: {e}")
    
    # Teste 2: Com filtro "Novo"
    print("\n2. Teste com filtro 'Novo':")
    try:
        messages_novo, acessos_novo = obter_dados_sms_com_filtro(data_ini, data_fim, "Novo")
        print(f"  - Mensagens retornadas: {len(messages_novo)}")
        print(f"  - Acessos retornados: {acessos_novo}")
        
        if messages_novo:
            primeira_msg = messages_novo[0]
            print(f"  - Campos da primeira mensagem: {list(primeira_msg.keys())}")
            if 'tenant_segment_id' in primeira_msg:
                print(f"  - tenant_segment_id: {primeira_msg['tenant_segment_id']}")
    except Exception as e:
        print(f"  - Erro: {e}")
    
    # Teste 3: Com filtro "FGTS"
    print("\n3. Teste com filtro 'FGTS':")
    try:
        messages_fgts, acessos_fgts = obter_dados_sms_com_filtro(data_ini, data_fim, "FGTS")
        print(f"  - Mensagens retornadas: {len(messages_fgts)}")
        print(f"  - Acessos retornados: {acessos_fgts}")
        
        if messages_fgts:
            primeira_msg = messages_fgts[0]
            print(f"  - Campos da primeira mensagem: {list(primeira_msg.keys())}")
            if 'tenant_segment_id' in primeira_msg:
                print(f"  - tenant_segment_id: {primeira_msg['tenant_segment_id']}")
    except Exception as e:
        print(f"  - Erro: {e}")
    
    # Teste 4: Com filtro "Crédito CLT"
    print("\n4. Teste com filtro 'Crédito CLT':")
    try:
        messages_clt, acessos_clt = obter_dados_sms_com_filtro(data_ini, data_fim, "Crédito CLT")
        print(f"  - Mensagens retornadas: {len(messages_clt)}")
        print(f"  - Acessos retornados: {acessos_clt}")
        
        if messages_clt:
            primeira_msg = messages_clt[0]
            print(f"  - Campos da primeira mensagem: {list(primeira_msg.keys())}")
            if 'tenant_segment_id' in primeira_msg:
                print(f"  - tenant_segment_id: {primeira_msg['tenant_segment_id']}")
    except Exception as e:
        print(f"  - Erro: {e}")
    
    # Teste 5: Teste direto da função de consulta
    print("\n5. Teste direto da função consultar_status_sms_kolmeya:")
    try:
        start_at = datetime.combine(data_ini, datetime.min.time()).strftime('%Y-%m-%d %H:%M')
        end_at = datetime.combine(data_fim, datetime.max.time()).strftime('%Y-%m-%d %H:%M')
        
        print(f"  - Testando com tenant_segment_id='Novo':")
        messages_direto = consultar_status_sms_kolmeya(start_at, end_at, limit=100, tenant_segment_id="Novo")
        print(f"    Mensagens retornadas: {len(messages_direto)}")
        
        print(f"  - Testando com tenant_segment_id=None:")
        messages_direto_todos = consultar_status_sms_kolmeya(start_at, end_at, limit=100, tenant_segment_id=None)
        print(f"    Mensagens retornadas: {len(messages_direto_todos)}")
        
    except Exception as e:
        print(f"  - Erro: {e}")

if __name__ == "__main__":
    test_filtros_api() 