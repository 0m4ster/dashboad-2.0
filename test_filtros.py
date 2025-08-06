import os
import sys

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do arquivo principal
from api_kolm import extrair_telefones_kolmeya, limpar_telefone

def test_extrair_telefones():
    """Testa a função de extração de telefones com dados simulados"""
    
    # Dados simulados do Kolmeya
    messages_todos = [
        {
            "id": 1,
            "phone": "11987654321",
            "cpf": "12345678901",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo"
        },
        {
            "id": 2,
            "phone": "11987654322",
            "cpf": "12345678902",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS"
        },
        {
            "id": 3,
            "phone": "11987654323",
            "cpf": "12345678903",
            "centro_custo": "Crédito CLT",
            "tenant_segment_id": "Crédito CLT"
        }
    ]
    
    messages_filtrados_novo = [
        {
            "id": 1,
            "phone": "11987654321",
            "cpf": "12345678901",
            "centro_custo": "Novo",
            "tenant_segment_id": "Novo"
        }
    ]
    
    messages_filtrados_fgts = [
        {
            "id": 2,
            "phone": "11987654322",
            "cpf": "12345678902",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS"
        }
    ]
    
    print("=== TESTE DE EXTRAÇÃO DE TELEFONES ===")
    
    # Teste com todos os dados
    print("\n1. Teste com TODOS os dados:")
    telefones_todos = extrair_telefones_kolmeya(messages_todos)
    print(f"Telefones encontrados: {telefones_todos}")
    print(f"Quantidade: {len(telefones_todos)}")
    
    # Teste com dados filtrados por "Novo"
    print("\n2. Teste com dados filtrados por 'Novo':")
    telefones_novo = extrair_telefones_kolmeya(messages_filtrados_novo)
    print(f"Telefones encontrados: {telefones_novo}")
    print(f"Quantidade: {len(telefones_novo)}")
    
    # Teste com dados filtrados por "FGTS"
    print("\n3. Teste com dados filtrados por 'FGTS':")
    telefones_fgts = extrair_telefones_kolmeya(messages_filtrados_fgts)
    print(f"Telefones encontrados: {telefones_fgts}")
    print(f"Quantidade: {len(telefones_fgts)}")
    
    # Verificar se os resultados fazem sentido
    print("\n=== VERIFICAÇÃO ===")
    print(f"Todos os telefones devem estar em 'todos': {telefones_todos}")
    print(f"Telefone do 'Novo' deve estar em 'todos': {'11987654321' in telefones_todos}")
    print(f"Telefone do 'FGTS' deve estar em 'todos': {'11987654322' in telefones_todos}")
    print(f"Telefone do 'CLT' deve estar em 'todos': {'11987654323' in telefones_todos}")
    
    # Teste com diferentes formatos de telefone
    print("\n=== TESTE COM DIFERENTES FORMATOS ===")
    messages_formatos = [
        {"phone": "11987654321"},
        {"phone": "(11) 98765-4321"},
        {"phone": "11 98765 4321"},
        {"phone": "+55 11 98765 4321"},
        {"cpf": "11987654321"},  # Telefone no campo CPF
        {"mobile": "11987654321"},
        {"celular": "11987654321"},
        {"number": "11987654321"},
        {"telefone": "11987654321"},
        {"phone_number": "11987654321"}
    ]
    
    telefones_formatos = extrair_telefones_kolmeya(messages_formatos)
    print(f"Telefones com diferentes formatos: {telefones_formatos}")
    print(f"Quantidade: {len(telefones_formatos)}")

if __name__ == "__main__":
    test_extrair_telefones() 