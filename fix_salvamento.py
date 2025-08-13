#!/usr/bin/env python3
"""
Script para corrigir o sistema de salvamento do dashboard
"""

import re

def fix_salvamento_system():
    """Corrige o sistema de salvamento no api_kolm.py"""
    
    # Ler o arquivo atual
    with open('api_kolm.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Padrão para encontrar a seção de salvamento
    pattern = r'# SALVAR MÉTRICAS NO BANCO DE DADOS\s+if HAS_DATABASE:\s+try:\s+# Preparar dados para salvar\s+dados_kolmeya = \{.*?\}\s+dados_4net = \{.*?\}\s+dados_whatsapp = \{.*?\}\s+dados_ad = \{.*?\}\s+# Salvar no banco de dados\s+salvar_metricas_dashboard\(.*?\)\s+except Exception as e:\s+print\(f"⚠️ Erro ao salvar métricas no banco: \{e\}"\)'
    
    # Novo código de salvamento melhorado
    new_salvamento = '''    # SALVAR MÉTRICAS NO BANCO DE DADOS - SISTEMA MELHORADO
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
                'leads_gerados': safe_int(telefones_base),
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
            traceback.print_exc()'''
    
    # Substituir usando regex mais específico
    pattern_simple = r'# SALVAR MÉTRICAS NO BANCO DE DADOS\s+if HAS_DATABASE:\s+try:\s+# Preparar dados para salvar\s+dados_kolmeya = \{.*?\}\s+dados_4net = \{.*?\}\s+dados_whatsapp = \{.*?\}\s+dados_ad = \{.*?\}\s+# Salvar no banco de dados\s+salvar_metricas_dashboard\(.*?\)\s+except Exception as e:\s+print\(f"⚠️ Erro ao salvar métricas no banco: \{e\}"\)'
    
    # Tentar substituição mais simples
    if re.search(pattern_simple, content, re.DOTALL):
        new_content = re.sub(pattern_simple, new_salvamento, content, flags=re.DOTALL)
        
        # Salvar o arquivo corrigido
        with open('api_kolm.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ Sistema de salvamento corrigido com sucesso!")
        return True
    else:
        print("❌ Não foi possível encontrar a seção de salvamento")
        return False

def fix_layout_centering():
    """Corrige o layout para melhor centralização"""
    
    # Ler o arquivo atual
    with open('api_kolm.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Padrão para encontrar o CSS do dashboard
    css_pattern = r'\.dashboard-container \{\s+display: flex;\s+gap: 15px;\s+margin: 10px 0;\s+flex-wrap: wrap;\s+justify-content: space-between;\s+\}'
    
    # Novo CSS com melhor centralização
    new_css = '''    .dashboard-container {
        display: flex;
        gap: 15px;
        margin: 10px auto;
        flex-wrap: wrap;
        justify-content: center;
        max-width: 1200px;
    }'''
    
    # Substituir o CSS
    if re.search(css_pattern, content):
        new_content = re.sub(css_pattern, new_css, content)
        
        # Salvar o arquivo corrigido
        with open('api_kolm.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ Layout centralizado corrigido!")
        return True
    else:
        print("❌ Não foi possível encontrar o CSS do dashboard")
        return False

if __name__ == "__main__":
    print("🔧 CORRIGINDO SISTEMA DE SALVAMENTO E LAYOUT")
    print("=" * 50)
    
    # Corrigir salvamento
    salvamento_ok = fix_salvamento_system()
    
    # Corrigir layout
    layout_ok = fix_layout_centering()
    
    if salvamento_ok and layout_ok:
        print("\n✅ Todas as correções aplicadas com sucesso!")
        print("\n🎯 Melhorias implementadas:")
        print("   ✅ Salvamento preciso com validação de dados")
        print("   ✅ Logs detalhados de salvamento")
        print("   ✅ Layout melhor centralizado")
        print("   ✅ Tratamento de erros robusto")
    else:
        print("\n⚠️ Algumas correções não puderam ser aplicadas")
        print("   Verifique o arquivo manualmente")
