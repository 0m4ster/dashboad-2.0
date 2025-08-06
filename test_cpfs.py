import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do arquivo principal
from api_kolm import extrair_cpfs_da_base, extrair_cpfs_kolmeya, comparar_cpfs

def test_cpfs():
    """Testa a funcionalidade de extração e comparação de CPFs"""
    
    print("=== TESTE DE CPFs ===")
    
    # Criar dados simulados da base
    dados_base = {
        'cpf': ['12345678901', '98765432100', '11122233344', '55566677788'],
        'telefone': ['11987654321', '11987654322', '11987654323', '11987654324'],
        'nome': ['João Silva', 'Maria Santos', 'Pedro Costa', 'Ana Oliveira']
    }
    df_base = pd.DataFrame(dados_base)
    
    # Dados simulados do Kolmeya
    messages_kolmeya = [
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
            "cpf": "98765432100",
            "centro_custo": "FGTS",
            "tenant_segment_id": "FGTS"
        },
        {
            "id": 3,
            "phone": "11987654325",
            "cpf": "99988877766",
            "centro_custo": "Crédito CLT",
            "tenant_segment_id": "Crédito CLT"
        }
    ]
    
    print("\n1. Testando extração de CPFs da base:")
    cpfs_base = extrair_cpfs_da_base(df_base)
    print(f"CPFs encontrados na base: {cpfs_base}")
    print(f"Quantidade: {len(cpfs_base)}")
    
    print("\n2. Testando extração de CPFs do Kolmeya:")
    cpfs_kolmeya = extrair_cpfs_kolmeya(messages_kolmeya)
    print(f"CPFs encontrados no Kolmeya: {cpfs_kolmeya}")
    print(f"Quantidade: {len(cpfs_kolmeya)}")
    
    print("\n3. Testando comparação de CPFs:")
    if cpfs_base and cpfs_kolmeya:
        resultado_cpfs = comparar_cpfs(cpfs_base, cpfs_kolmeya)
        
        print(f"CPFs enviados: {resultado_cpfs['enviados']}")
        print(f"CPFs não enviados: {resultado_cpfs['nao_enviados']}")
        print(f"CPFs extra: {resultado_cpfs['extra']}")
        
        print(f"\nResumo:")
        print(f"- Total na base: {resultado_cpfs['total_base']}")
        print(f"- Total no Kolmeya: {resultado_cpfs['total_kolmeya']}")
        print(f"- Enviados: {resultado_cpfs['total_enviados']}")
        print(f"- Não enviados: {resultado_cpfs['total_nao_enviados']}")
        print(f"- Extra: {resultado_cpfs['total_extra']}")
        
        # Taxa de cobertura
        if resultado_cpfs['total_base'] > 0:
            taxa_cobertura = (resultado_cpfs['total_enviados'] / resultado_cpfs['total_base']) * 100
            print(f"- Taxa de cobertura: {taxa_cobertura:.1f}%")
    
    # Teste com diferentes formatos de CPF
    print("\n4. Testando diferentes formatos de CPF:")
    dados_formatos = {
        'cpf': [
            '12345678901',  # Sem formatação
            '123.456.789-01',  # Com pontos e hífen
            '123456789-01',  # Com hífen
            '123.45678901',  # Com ponto
            '98765432100',  # Outro CPF
            '00012345678'  # CPF que começa com 0 (deve ser ignorado)
        ]
    }
    df_formatos = pd.DataFrame(dados_formatos)
    
    cpfs_formatos = extrair_cpfs_da_base(df_formatos)
    print(f"CPFs com diferentes formatos: {cpfs_formatos}")
    print(f"Quantidade: {len(cpfs_formatos)}")
    
    # Teste com dados do Kolmeya com diferentes formatos
    messages_formatos = [
        {"cpf": "12345678901"},
        {"cpf": "123.456.789-01"},
        {"cpf": "98765432100"},
        {"document": "11122233344"},
        {"documento": "55566677788"}
    ]
    
    cpfs_kolmeya_formatos = extrair_cpfs_kolmeya(messages_formatos)
    print(f"CPFs do Kolmeya com diferentes formatos: {cpfs_kolmeya_formatos}")
    print(f"Quantidade: {len(cpfs_kolmeya_formatos)}")

if __name__ == "__main__":
    test_cpfs() 