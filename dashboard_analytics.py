import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os

# Importar gerenciador de banco de dados
try:
    from database_manager import DashboardDatabase
    HAS_DATABASE = True
except ImportError:
    HAS_DATABASE = False
    st.error("❌ Módulo de banco de dados não encontrado!")

def main():
    st.set_page_config(page_title="Dashboard Analytics", layout="wide")
    
    st.markdown("<h1 style='text-align: center;'>📊 Dashboard Analytics - Histórico</h1>", unsafe_allow_html=True)
    
    if not HAS_DATABASE:
        st.error("❌ Sistema de banco de dados não disponível!")
        return
    
    # Inicializar banco de dados
    db = DashboardDatabase()
    
    # Sidebar para filtros
    st.sidebar.markdown("### 🔍 Filtros")
    
    # Filtro de período
    periodo_opcoes = {
        "Últimos 7 dias": 7,
        "Últimos 30 dias": 30,
        "Últimos 90 dias": 90,
        "Personalizado": 0
    }
    
    periodo_selecionado = st.sidebar.selectbox(
        "Período",
        options=list(periodo_opcoes.keys()),
        index=1
    )
    
    if periodo_opcoes[periodo_selecionado] > 0:
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=periodo_opcoes[periodo_selecionado])
    else:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            data_inicio = st.date_input("Data Início", value=datetime.now().replace(day=1).date())
        with col2:
            data_fim = st.date_input("Data Fim", value=datetime.now().date())
        
        data_inicio = datetime.combine(data_inicio, datetime.min.time())
        data_fim = datetime.combine(data_fim, datetime.max.time())
    
    # Filtro de centro de custo
    centro_custo_opcoes = ["TODOS", "Novo", "FGTS", "Crédito CLT"]
    centro_custo_selecionado = st.sidebar.selectbox(
        "Centro de Custo",
        options=centro_custo_opcoes,
        index=0
    )
    
    # Filtro de canal
    canal_opcoes = ["TODOS", "Kolmeya", "4NET", "WhatsApp", "AD"]
    canal_selecionado = st.sidebar.selectbox(
        "Canal",
        options=canal_opcoes,
        index=0
    )
    
    # Botões de ação
    st.sidebar.markdown("### 🛠️ Ações")
    
    if st.sidebar.button("🔄 Atualizar Dados"):
        st.rerun()
    
    if st.sidebar.button("📊 Exportar CSV"):
        arquivo_export = db.exportar_dados(data_inicio, data_fim, 'csv', centro_custo_selecionado)
        if arquivo_export:
            with open(arquivo_export, 'r', encoding='utf-8-sig') as f:
                st.sidebar.download_button(
                    label="💾 Download CSV",
                    data=f.read(),
                    file_name=arquivo_export,
                    mime="text/csv"
                )
    
    if st.sidebar.button("🧹 Limpar Dados Antigos"):
        if db.limpar_dados_antigos(90):
            st.sidebar.success("✅ Dados antigos removidos!")
        else:
            st.sidebar.error("❌ Erro ao limpar dados!")
    
    # Conteúdo principal
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📅 Período", f"{data_inicio.strftime('%d/%m')} - {data_fim.strftime('%d/%m')}")
    
    with col2:
        st.metric("🏢 Centro de Custo", centro_custo_selecionado)
    
    with col3:
        st.metric("📡 Canal", canal_selecionado)
    
    with col4:
        stats = db.obter_estatisticas_gerais()
        st.metric("💾 Tamanho DB", f"{stats.get('tamanho_banco_mb', 0)} MB")
    
    # Métricas gerais
    st.markdown("### 📈 Métricas Gerais")
    
    metricas_gerais = db.obter_metricas_gerais(data_inicio, data_fim, centro_custo_selecionado)
    
    if metricas_gerais and metricas_gerais.get('totais'):
        totais = metricas_gerais['totais']
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("📱 Total SMS", f"{totais.get('total_sms', 0):,}")
        
        with col2:
            st.metric("💰 Total Produção", f"R$ {totais.get('total_producao', 0):,.2f}")
        
        with col3:
            st.metric("💵 Total Investimento", f"R$ {totais.get('total_investimento', 0):,.2f}")
        
        with col4:
            st.metric("📊 Total ROI", f"R$ {totais.get('total_roi', 0):,.2f}")
        
        with col5:
            st.metric("🎯 Ticket Médio", f"R$ {totais.get('ticket_medio_geral', 0):,.2f}")
    
    # Gráficos por canal
    st.markdown("### 📊 Análise por Canal")
    
    if metricas_gerais and metricas_gerais.get('por_canal'):
        df_canal = pd.DataFrame(metricas_gerais['por_canal'])
        
        # Gráfico de produção por canal
        fig_producao = px.bar(
            df_canal, 
            x='canal', 
            y='media_producao',
            title="💰 Produção Média por Canal",
            labels={'media_producao': 'Produção Média (R$)', 'canal': 'Canal'},
            color='canal'
        )
        fig_producao.update_layout(showlegend=False)
        st.plotly_chart(fig_producao, use_container_width=True)
        
        # Gráfico de ROI por canal
        col1, col2 = st.columns(2)
        
        with col1:
            fig_roi = px.bar(
                df_canal, 
                x='canal', 
                y='media_roi',
                title="📈 ROI Médio por Canal",
                labels={'media_roi': 'ROI Médio (R$)', 'canal': 'Canal'},
                color='canal'
            )
            fig_roi.update_layout(showlegend=False)
            st.plotly_chart(fig_roi, use_container_width=True)
        
        with col2:
            fig_investimento = px.bar(
                df_canal, 
                x='canal', 
                y='media_investimento',
                title="💵 Investimento Médio por Canal",
                labels={'media_investimento': 'Investimento Médio (R$)', 'canal': 'Canal'},
                color='canal'
            )
            fig_investimento.update_layout(showlegend=False)
            st.plotly_chart(fig_investimento, use_container_width=True)
    
    # Tabela detalhada de métricas
    st.markdown("### 📋 Métricas Detalhadas")
    
    if canal_selecionado == "TODOS":
        # Mostrar métricas de todos os canais
        for canal in ["Kolmeya", "4NET", "WhatsApp", "AD"]:
            df_metricas = db.obter_metricas_por_periodo(
                canal, data_inicio, data_fim, centro_custo_selecionado
            )
            
            if not df_metricas.empty:
                st.markdown(f"#### {canal}")
                
                # Selecionar colunas relevantes
                colunas_exibir = [
                    'data_coleta', 'sms_enviados', 'interacoes', 'investimento',
                    'taxa_entrega', 'total_vendas', 'producao', 'leads_gerados',
                    'ticket_medio', 'roi'
                ]
                
                df_exibir = df_metricas[colunas_exibir].copy()
                df_exibir['data_coleta'] = pd.to_datetime(df_exibir['data_coleta']).dt.strftime('%d/%m/%Y %H:%M')
                
                # Formatar valores monetários
                for col in ['investimento', 'producao', 'ticket_medio', 'roi']:
                    if col in df_exibir.columns:
                        df_exibir[col] = df_exibir[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "R$ 0,00")
                
                st.dataframe(df_exibir, use_container_width=True)
                st.markdown("---")
    else:
        # Mostrar métricas do canal selecionado
        df_metricas = db.obter_metricas_por_periodo(
            canal_selecionado, data_inicio, data_fim, centro_custo_selecionado
        )
        
        if not df_metricas.empty:
            # Selecionar colunas relevantes
            colunas_exibir = [
                'data_coleta', 'sms_enviados', 'interacoes', 'investimento',
                'taxa_entrega', 'total_vendas', 'producao', 'leads_gerados',
                'ticket_medio', 'roi'
            ]
            
            df_exibir = df_metricas[colunas_exibir].copy()
            df_exibir['data_coleta'] = pd.to_datetime(df_exibir['data_coleta']).dt.strftime('%d/%m/%Y %H:%M')
            
            # Formatar valores monetários
            for col in ['investimento', 'producao', 'ticket_medio', 'roi']:
                if col in df_exibir.columns:
                    df_exibir[col] = df_exibir[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "R$ 0,00")
            
            st.dataframe(df_exibir, use_container_width=True)
        else:
            st.info(f"📭 Nenhuma métrica encontrada para {canal_selecionado} no período selecionado.")
    
    # Histórico de consultas Facta
    st.markdown("### 🔍 Histórico de Consultas Facta")
    
    if canal_selecionado == "TODOS":
        for canal in ["Kolmeya", "4NET", "WhatsApp", "AD"]:
            df_facta = db.obter_historico_facta(
                canal, data_inicio, data_fim, centro_custo_selecionado
            )
            
            if not df_facta.empty:
                st.markdown(f"#### {canal}")
                
                colunas_facta = [
                    'data_consulta', 'cpfs_consultados', 'propostas_encontradas',
                    'valor_total_propostas', 'taxa_conversao'
                ]
                
                df_facta_exibir = df_facta[colunas_facta].copy()
                df_facta_exibir['data_consulta'] = pd.to_datetime(df_facta_exibir['data_consulta']).dt.strftime('%d/%m/%Y %H:%M')
                df_facta_exibir['valor_total_propostas'] = df_facta_exibir['valor_total_propostas'].apply(
                    lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "R$ 0,00"
                )
                df_facta_exibir['taxa_conversao'] = df_facta_exibir['taxa_conversao'].apply(
                    lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%"
                )
                
                st.dataframe(df_facta_exibir, use_container_width=True)
                st.markdown("---")
    else:
        df_facta = db.obter_historico_facta(
            canal_selecionado, data_inicio, data_fim, centro_custo_selecionado
        )
        
        if not df_facta.empty:
            colunas_facta = [
                'data_consulta', 'cpfs_consultados', 'propostas_encontradas',
                'valor_total_propostas', 'taxa_conversao'
            ]
            
            df_facta_exibir = df_facta[colunas_facta].copy()
            df_facta_exibir['data_consulta'] = pd.to_datetime(df_facta_exibir['data_consulta']).dt.strftime('%d/%m/%Y %H:%M')
            df_facta_exibir['valor_total_propostas'] = df_facta_exibir['valor_total_propostas'].apply(
                lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "R$ 0,00"
            )
            df_facta_exibir['taxa_conversao'] = df_facta_exibir['taxa_conversao'].apply(
                lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%"
            )
            
            st.dataframe(df_facta_exibir, use_container_width=True)
        else:
            st.info(f"📭 Nenhuma consulta Facta encontrada para {canal_selecionado} no período selecionado.")
    
    # Estatísticas do banco
    st.markdown("### 💾 Estatísticas do Banco de Dados")
    
    stats = db.obter_estatisticas_gerais()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Métricas", stats.get('total_metricas', 0))
    
    with col2:
        st.metric("🔍 Total Consultas Facta", stats.get('total_consultas', 0))
    
    with col3:
        ultima_atualizacao = stats.get('ultima_atualizacao', 'N/A')
        if ultima_atualizacao and ultima_atualizacao != 'N/A':
            ultima_atualizacao = pd.to_datetime(ultima_atualizacao).strftime('%d/%m/%Y %H:%M')
        st.metric("🕒 Última Atualização", ultima_atualizacao)
    
    with col4:
        st.metric("💾 Tamanho do Banco", f"{stats.get('tamanho_banco_mb', 0)} MB")

if __name__ == "__main__":
    main()
