import os
import sys
from datetime import datetime, timedelta

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do arquivo principal
from api_kolm import extrair_telefones_kolmeya

def test_filtros_simulados():
    """Testa a lógica de filtragem com dados simulados"""
    
    # Dados simulados do Kolmeya
    messages_todos = [
        {
            "id": 1,
            "phone": "11987654321",
            "cpf": "12345678901",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo",
            "status": "delivered"
        },
        {
            "id": 2,
            "phone": "11987654322",
            "cpf": "12345678902",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS",
            "status": "delivered"
        },
        {
            "id": 3,
            "phone": "11987654323",
            "cpf": "12345678903",
            "centro_custo": "Crédito CLT",
            "tenant_segment_id": "Crédito CLT",
            "status": "delivered"
        },
        {
            "id": 4,
            "phone": "11987654324",
            "cpf": "12345678904",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo",
            "status": "delivered"
        }
    ]
    
    print("=== TESTE COM DADOS SIMULADOS ===")
    print(f"Total de mensagens simuladas: {len(messages_todos)}")
    
    # Simular filtros
    filtros = {
        "TODOS": None,
        "Novo": "Novo",
        "FGTS": "FGTS", 
        "Crédito CLT": "Crédito CLT"
    }
    
    for nome_filtro, valor_filtro in filtros.items():
        print(f"\n--- Teste com filtro: {nome_filtro} ---")
        
        # Simular filtragem
        if valor_filtro is None:
            # Sem filtro - usa todas as mensagens
            messages_filtradas = messages_todos
        else:
            # Com filtro - filtra por tenant_segment_id
            messages_filtradas = []
            for msg in messages_todos:
                if msg.get('tenant_segment_id') == valor_filtro:
                    messages_filtradas.append(msg)
        
        print(f"Mensagens após filtro: {len(messages_filtradas)}")
        
        # Extrair telefones
        telefones = extrair_telefones_kolmeya(messages_filtradas)
        print(f"Telefones extraídos: {len(telefones)}")
        print(f"Lista de telefones: {sorted(telefones)}")
        
        # Mostrar detalhes das mensagens filtradas
        for i, msg in enumerate(messages_filtradas):
            print(f"  Mensagem {i+1}: ID={msg['id']}, Phone={msg['phone']}, Tenant={msg['tenant_segment_id']}")
    
    # Teste de cenários específicos
    print("\n=== CENÁRIOS ESPECÍFICOS ===")
    
    # Cenário 1: Filtro "Novo" deve retornar 2 mensagens
    messages_novo = [msg for msg in messages_todos if msg.get('tenant_segment_id') == 'Novo']
    telefones_novo = extrair_telefones_kolmeya(messages_novo)
    print(f"Cenário 'Novo': {len(messages_novo)} mensagens, {len(telefones_novo)} telefones")
    
    # Cenário 2: Filtro "FGTS" deve retornar 1 mensagem
    messages_fgts = [msg for msg in messages_todos if msg.get('tenant_segment_id') == 'FGTS']
    telefones_fgts = extrair_telefones_kolmeya(messages_fgts)
    print(f"Cenário 'FGTS': {len(messages_fgts)} mensagens, {len(telefones_fgts)} telefones")
    
    # Cenário 3: Filtro "Crédito CLT" deve retornar 1 mensagem
    messages_clt = [msg for msg in messages_todos if msg.get('tenant_segment_id') == 'Crédito CLT']
    telefones_clt = extrair_telefones_kolmeya(messages_clt)
    print(f"Cenário 'Crédito CLT': {len(messages_clt)} mensagens, {len(telefones_clt)} telefones")
    
    # Verificação final
    print("\n=== VERIFICAÇÃO FINAL ===")
    telefones_todos = extrair_telefones_kolmeya(messages_todos)
    print(f"Total de telefones únicos: {len(telefones_todos)}")
    print(f"Telefones: {sorted(telefones_todos)}")
    
    # Verificar se a soma dos filtros individuais é igual ao total
    telefones_soma = telefones_novo.union(telefones_fgts).union(telefones_clt)
    print(f"Telefones da soma dos filtros: {len(telefones_soma)}")
    print(f"São iguais? {telefones_todos == telefones_soma}")

if __name__ == "__main__":
    test_filtros_simulados() 