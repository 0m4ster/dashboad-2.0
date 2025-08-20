#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from datetime import datetime, timedelta

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do api_kolm.py
from api_kolm import (
    get_facta_token,
    consultar_andamento_propostas_facta,
    consultar_andamento_propostas_por_acessos,
    get_kolmeya_token
)

def testar_token_facta():
    """Testa se o token do FACTA está funcionando"""
    print("🔍 Testando token do FACTA...")
    
    token = get_facta_token()
    if token:
        print(f"✅ Token do FACTA encontrado: {token[:20]}...")
        return token
    else:
        print("❌ Token do FACTA não encontrado!")
        return None

def testar_consulta_direta_facta():
    """Testa consulta direta no FACTA com CPFs de exemplo"""
    print("\n🔍 Testando consulta direta no FACTA...")
    
    # CPFs de exemplo para teste
    cpfs_teste = {"12345678901", "98765432100"}
    
    try:
        propostas = consultar_andamento_propostas_facta(
            cpfs=cpfs_teste,
            ambiente="homologacao"
        )
        
        print(f"✅ Consulta direta concluída!")
        print(f"   📊 Propostas encontradas: {len(propostas)}")
        
        if propostas:
            print("   📋 Primeiras propostas:")
            for i, proposta in enumerate(propostas[:3]):
                print(f"      {i+1}. CPF: {proposta.get('cpf', 'N/A')}")
                print(f"         Cliente: {proposta.get('cliente', 'N/A')}")
                print(f"         Valor AF: R$ {proposta.get('valor_af', 0):,.2f}")
                print(f"         Status: {proposta.get('status_proposta', 'N/A')}")
                print()
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na consulta direta: {e}")
        import traceback
        traceback.print_exc()
        return False

def testar_consulta_integrada():
    """Testa a consulta integrada Kolmeya + FACTA"""
    print("\n🔍 Testando consulta integrada Kolmeya + FACTA...")
    
    # Definir período de teste (últimos 7 dias)
    end_at = datetime.now().strftime('%Y-%m-%d')
    start_at = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    print(f"   📅 Período de teste: {start_at} até {end_at}")
    
    try:
        resultado = consultar_andamento_propostas_por_acessos(
            start_at=start_at,
            end_at=end_at,
            limit=100,  # Limitar para teste
            ambiente_facta="homologacao"
        )
        
        print(f"✅ Consulta integrada concluída!")
        print(f"   📱 Acessos encontrados: {resultado['cpfs_encontrados']}")
        print(f"   🆔 CPFs únicos: {resultado['cpfs_encontrados']}")
        print(f"   📋 Propostas encontradas: {resultado['propostas_encontradas']}")
        
        if resultado['propostas']:
            print("   📋 Primeiras propostas:")
            for i, proposta in enumerate(resultado['propostas'][:3]):
                print(f"      {i+1}. CPF: {proposta.get('cpf', 'N/A')}")
                print(f"         Cliente: {proposta.get('cliente', 'N/A')}")
                print(f"         Valor AF: R$ {proposta.get('valor_af', 0):,.2f}")
                print(f"         Status: {proposta.get('status_proposta', 'N/A')}")
                print()
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na consulta integrada: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Função principal de teste"""
    print("🚀 Iniciando testes da integração FACTA...")
    print("=" * 50)
    
    # Teste 1: Token do FACTA
    token_facta = testar_token_facta()
    if not token_facta:
        print("\n❌ Falha no teste do token. Verifique se o arquivo facta_token.txt existe.")
        return
    
    # Teste 2: Consulta direta
    sucesso_direta = testar_consulta_direta_facta()
    
    # Teste 3: Consulta integrada
    sucesso_integrada = testar_consulta_integrada()
    
    # Resumo final
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES:")
    print(f"   🔑 Token FACTA: {'✅ OK' if token_facta else '❌ FALHOU'}")
    print(f"   🔍 Consulta Direta: {'✅ OK' if sucesso_direta else '❌ FALHOU'}")
    print(f"   🔗 Consulta Integrada: {'✅ OK' if sucesso_integrada else '❌ FALHOU'}")
    
    if token_facta and sucesso_direta and sucesso_integrada:
        print("\n🎉 Todos os testes passaram! A integração está funcionando.")
    else:
        print("\n⚠️ Alguns testes falharam. Verifique os logs acima.")

if __name__ == "__main__":
    main()
