import os
import sys
from datetime import datetime, timedelta

# Adicionar o diretório atual ao path para importar as funções
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar as funções do arquivo principal
from api_kolm import obter_dados_sms_com_filtro, extrair_telefones_kolmeya

def test_filtros_final():
    """Teste final para verificar se todos os filtros estão funcionando"""
    
    # Definir período de teste
    data_fim = datetime.now().date()
    data_ini = data_fim - timedelta(days=7)
    
    print("=== TESTE FINAL DOS FILTROS ===")
    print(f"Período: {data_ini} até {data_fim}")
    
    # Testar todos os filtros
    filtros = {
        "TODOS": None,
        "Novo": "Novo",
        "FGTS": "FGTS",
        "Crédito CLT": "Crédito CLT"
    }
    
    resultados = {}
    
    for nome_filtro, valor_filtro in filtros.items():
        print(f"\n--- Testando filtro: {nome_filtro} ---")
        
        try:
            # Obter dados com filtro
            messages, total_acessos = obter_dados_sms_com_filtro(data_ini, data_fim, valor_filtro)
            
            # Extrair telefones
            telefones = extrair_telefones_kolmeya(messages)
            
            # Armazenar resultados
            resultados[nome_filtro] = {
                'mensagens': len(messages),
                'telefones': len(telefones),
                'acessos': total_acessos,
                'telefones_lista': sorted(telefones)
            }
            
            print(f"  ✅ Mensagens: {len(messages)}")
            print(f"  ✅ Telefones: {len(telefones)}")
            print(f"  ✅ Acessos: {total_acessos}")
            print(f"  ✅ Telefones: {sorted(telefones)}")
            
        except Exception as e:
            print(f"  ❌ Erro: {e}")
            resultados[nome_filtro] = {
                'mensagens': 0,
                'telefones': 0,
                'acessos': 0,
                'telefones_lista': []
            }
    
    # Resumo dos resultados
    print("\n" + "="*50)
    print("RESUMO DOS RESULTADOS")
    print("="*50)
    
    for nome_filtro, resultado in resultados.items():
        status = "✅" if resultado['mensagens'] > 0 else "❌"
        print(f"{status} {nome_filtro}: {resultado['mensagens']} SMS, {resultado['telefones']} telefones, {resultado['acessos']} acessos")
    
    # Verificar se os filtros específicos estão retornando dados
    print("\n" + "="*50)
    print("VERIFICAÇÃO DE FUNCIONAMENTO")
    print("="*50)
    
    todos_telefones = set(resultados["TODOS"]["telefones_lista"])
    
    for nome_filtro, resultado in resultados.items():
        if nome_filtro != "TODOS":
            filtro_telefones = set(resultado["telefones_lista"])
            
            # Verificar se os telefones do filtro estão no total
            telefones_no_total = filtro_telefones.issubset(todos_telefones)
            
            # Verificar se há telefones específicos para este filtro
            tem_telefones_especificos = len(filtro_telefones) > 0
            
            status = "✅" if telefones_no_total and tem_telefones_especificos else "❌"
            print(f"{status} {nome_filtro}: {len(filtro_telefones)} telefones específicos")
    
    print("\n" + "="*50)
    print("CONCLUSÃO")
    print("="*50)
    
    filtros_funcionando = 0
    for nome_filtro, resultado in resultados.items():
        if resultado['mensagens'] > 0 and resultado['telefones'] > 0:
            filtros_funcionando += 1
    
    if filtros_funcionando == len(filtros):
        print("🎉 TODOS OS FILTROS ESTÃO FUNCIONANDO CORRETAMENTE!")
    else:
        print(f"⚠️  {filtros_funcionando}/{len(filtros)} filtros estão funcionando")
    
    return resultados

if __name__ == "__main__":
    test_filtros_final() 