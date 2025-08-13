import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Tuple

class DashboardDatabase:
    def __init__(self, db_path: str = "dashboard.db"):
        """Inicializa o gerenciador do banco de dados."""
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializa o banco de dados e cria as tabelas necessárias."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabela principal de métricas
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metricas_dashboard (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canal TEXT NOT NULL,
            sms_enviados INTEGER DEFAULT 0,
            interacoes REAL DEFAULT 0.0,
            investimento REAL DEFAULT 0.0,
            taxa_entrega REAL DEFAULT 0.0,
            total_vendas INTEGER DEFAULT 0,
            producao REAL DEFAULT 0.0,
            leads_gerados INTEGER DEFAULT 0,
            ticket_medio REAL DEFAULT 0.0,
            roi REAL DEFAULT 0.0,
            data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            centro_custo TEXT DEFAULT 'TODOS',
            periodo_inicio DATE,
            periodo_fim DATE
        )
        """)
        
        # Tabela para histórico de consultas da Facta
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS consultas_facta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canal TEXT NOT NULL,
            cpfs_consultados INTEGER DEFAULT 0,
            propostas_encontradas INTEGER DEFAULT 0,
            valor_total_propostas REAL DEFAULT 0.0,
            taxa_conversao REAL DEFAULT 0.0,
            data_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            centro_custo TEXT DEFAULT 'TODOS',
            periodo_inicio DATE,
            periodo_fim DATE
        )
        """)
        
        # Tabela para configurações do sistema
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS configuracoes (
            chave TEXT PRIMARY KEY,
            valor TEXT,
            descricao TEXT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Inserir configurações padrão
        cursor.execute("""
        INSERT OR IGNORE INTO configuracoes (chave, valor, descricao) VALUES 
        ('ultima_atualizacao', '', 'Data da última atualização do dashboard'),
        ('versao_sistema', '1.0', 'Versão atual do sistema'),
        ('retencao_dados_dias', '90', 'Dias para reter dados históricos')
        """)
        
        conn.commit()
        conn.close()
        print(f"✅ Banco de dados inicializado: {self.db_path}")
    
    def salvar_metricas(self, dados: Dict, centro_custo: str = "TODOS", 
                        periodo_inicio: Optional[datetime] = None, 
                        periodo_fim: Optional[datetime] = None) -> bool:
        """Salva as métricas do dashboard no banco de dados."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Preparar dados para inserção
            dados_insert = {
                'canal': dados.get('canal', ''),
                'sms_enviados': dados.get('sms_enviados', 0),
                'interacoes': dados.get('interacoes', 0.0),
                'investimento': dados.get('investimento', 0.0),
                'taxa_entrega': dados.get('taxa_entrega', 0.0),
                'total_vendas': dados.get('total_vendas', 0),
                'producao': dados.get('producao', 0.0),
                'leads_gerados': dados.get('leads_gerados', 0),
                'ticket_medio': dados.get('ticket_medio', 0.0),
                'roi': dados.get('roi', 0.0),
                'centro_custo': centro_custo,
                'periodo_inicio': periodo_inicio.date() if periodo_inicio else None,
                'periodo_fim': periodo_fim.date() if periodo_fim else None
            }
            
            cursor.execute("""
            INSERT INTO metricas_dashboard (
                canal, sms_enviados, interacoes, investimento, taxa_entrega,
                total_vendas, producao, leads_gerados, ticket_medio, roi,
                centro_custo, periodo_inicio, periodo_fim
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dados_insert['canal'], dados_insert['sms_enviados'], dados_insert['interacoes'],
                dados_insert['investimento'], dados_insert['taxa_entrega'], dados_insert['total_vendas'],
                dados_insert['producao'], dados_insert['leads_gerados'], dados_insert['ticket_medio'],
                dados_insert['roi'], dados_insert['centro_custo'], dados_insert['periodo_inicio'],
                dados_insert['periodo_fim']
            ))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Métricas salvas para {dados_insert['canal']} - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao salvar métricas: {e}")
            return False
    
    def salvar_consulta_facta(self, dados: Dict, centro_custo: str = "TODOS",
                              periodo_inicio: Optional[datetime] = None,
                              periodo_fim: Optional[datetime] = None) -> bool:
        """Salva os resultados de consultas da Facta."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO consultas_facta (
                canal, cpfs_consultados, propostas_encontradas, valor_total_propostas,
                taxa_conversao, centro_custo, periodo_inicio, periodo_fim
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dados.get('canal', ''),
                dados.get('cpfs_consultados', 0),
                dados.get('propostas_encontradas', 0),
                dados.get('valor_total_propostas', 0.0),
                dados.get('taxa_conversao', 0.0),
                centro_custo,
                periodo_inicio.date() if periodo_inicio else None,
                periodo_fim.date() if periodo_fim else None
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Erro ao salvar consulta Facta: {e}")
            return False
    
    def obter_metricas_por_periodo(self, canal: str, data_inicio: datetime, 
                                   data_fim: datetime, centro_custo: str = "TODOS") -> pd.DataFrame:
        """Obtém métricas de um canal específico em um período."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
            SELECT * FROM metricas_dashboard 
            WHERE canal = ? 
            AND data_coleta BETWEEN ? AND ?
            AND (centro_custo = ? OR centro_custo = 'TODOS')
            ORDER BY data_coleta DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(
                canal, data_inicio, data_fim, centro_custo
            ))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"❌ Erro ao consultar métricas: {e}")
            return pd.DataFrame()
    
    def obter_metricas_gerais(self, data_inicio: datetime, data_fim: datetime,
                              centro_custo: str = "TODOS") -> Dict:
        """Obtém métricas gerais de todos os canais em um período."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Métricas por canal
            query_canal = """
            SELECT canal, 
                   AVG(sms_enviados) as media_sms,
                   AVG(interacoes) as media_interacoes,
                   AVG(investimento) as media_investimento,
                   AVG(producao) as media_producao,
                   AVG(roi) as media_roi,
                   COUNT(*) as total_registros
            FROM metricas_dashboard 
            WHERE data_coleta BETWEEN ? AND ?
            AND (centro_custo = ? OR centro_custo = 'TODOS')
            GROUP BY canal
            """
            
            df_canal = pd.read_sql_query(query_canal, conn, params=(
                data_inicio, data_fim, centro_custo
            ))
            
            # Totais gerais
            query_total = """
            SELECT 
                SUM(sms_enviados) as total_sms,
                SUM(producao) as total_producao,
                SUM(investimento) as total_investimento,
                SUM(roi) as total_roi,
                AVG(ticket_medio) as ticket_medio_geral
            FROM metricas_dashboard 
            WHERE data_coleta BETWEEN ? AND ?
            AND (centro_custo = ? OR centro_custo = 'TODOS')
            """
            
            df_total = pd.read_sql_query(query_total, conn, params=(
                data_inicio, data_fim, centro_custo
            ))
            
            conn.close()
            
            return {
                'por_canal': df_canal.to_dict('records'),
                'totais': df_total.to_dict('records')[0] if not df_total.empty else {}
            }
            
        except Exception as e:
            print(f"❌ Erro ao consultar métricas gerais: {e}")
            return {}
    
    def obter_historico_facta(self, canal: str, data_inicio: datetime,
                              data_fim: datetime, centro_custo: str = "TODOS") -> pd.DataFrame:
        """Obtém histórico de consultas da Facta."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
            SELECT * FROM consultas_facta 
            WHERE canal = ? 
            AND data_consulta BETWEEN ? AND ?
            AND (centro_custo = ? OR centro_custo = 'TODOS')
            ORDER BY data_consulta DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(
                canal, data_inicio, data_fim, centro_custo
            ))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"❌ Erro ao consultar histórico Facta: {e}")
            return pd.DataFrame()
    
    def limpar_dados_antigos(self, dias_retencao: int = 90):
        """Remove dados mais antigos que o período de retenção."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            data_limite = datetime.now() - timedelta(days=dias_retencao)
            
            # Limpar métricas antigas
            cursor.execute("""
            DELETE FROM metricas_dashboard 
            WHERE data_coleta < ?
            """, (data_limite,))
            
            # Limpar consultas Facta antigas
            cursor.execute("""
            DELETE FROM consultas_facta 
            WHERE data_consulta < ?
            """, (data_limite,))
            
            registros_removidos = cursor.rowcount
            conn.commit()
            conn.close()
            
            print(f"🧹 Dados antigos removidos: {registros_removidos} registros")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao limpar dados antigos: {e}")
            return False
    
    def obter_estatisticas_gerais(self) -> Dict:
        """Obtém estatísticas gerais do banco de dados."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total de registros por tabela
            cursor.execute("SELECT COUNT(*) FROM metricas_dashboard")
            total_metricas = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM consultas_facta")
            total_consultas = cursor.fetchone()[0]
            
            # Última atualização
            cursor.execute("SELECT MAX(data_coleta) FROM metricas_dashboard")
            ultima_atualizacao = cursor.fetchone()[0]
            
            # Tamanho do banco
            cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            tamanho_banco = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_metricas': total_metricas,
                'total_consultas': total_consultas,
                'ultima_atualizacao': ultima_atualizacao,
                'tamanho_banco_mb': round(tamanho_banco / (1024 * 1024), 2)
            }
            
        except Exception as e:
            print(f"❌ Erro ao obter estatísticas: {e}")
            return {}
    
    def exportar_dados(self, data_inicio: datetime, data_fim: datetime,
                       formato: str = 'csv', centro_custo: str = "TODOS") -> str:
        """Exporta dados para CSV ou Excel."""
        try:
            # Obter métricas
            df_metricas = pd.read_sql_query("""
                SELECT * FROM metricas_dashboard 
                WHERE data_coleta BETWEEN ? AND ?
                AND (centro_custo = ? OR centro_custo = 'TODOS')
                ORDER BY data_coleta DESC
            """, sqlite3.connect(self.db_path), params=(data_inicio, data_fim, centro_custo))
            
            # Obter consultas Facta
            df_consultas = pd.read_sql_query("""
                SELECT * FROM consultas_facta 
                WHERE data_consulta BETWEEN ? AND ?
                AND (centro_custo = ? OR centro_custo = 'TODOS')
                ORDER BY data_consulta DESC
            """, sqlite3.connect(self.db_path), params=(data_inicio, data_fim, centro_custo))
            
            # Nome do arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo = f"dashboard_export_{timestamp}"
            
            if formato.lower() == 'csv':
                nome_arquivo += ".csv"
                df_metricas.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig')
            elif formato.lower() == 'excel':
                nome_arquivo += ".xlsx"
                with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
                    df_metricas.to_excel(writer, sheet_name='Métricas', index=False)
                    df_consultas.to_excel(writer, sheet_name='Consultas_Facta', index=False)
            
            print(f"📊 Dados exportados: {nome_arquivo}")
            return nome_arquivo
            
        except Exception as e:
            print(f"❌ Erro ao exportar dados: {e}")
            return ""

# Função de conveniência para uso no dashboard
def salvar_metricas_dashboard(dados_kolmeya: Dict, dados_4net: Dict, 
                             dados_whatsapp: Dict, dados_ad: Dict,
                             centro_custo: str = "TODOS",
                             periodo_inicio: Optional[datetime] = None,
                             periodo_fim: Optional[datetime] = None):
    """Função de conveniência para salvar todas as métricas do dashboard."""
    db = DashboardDatabase()
    
    # Salvar métricas de cada canal
    if dados_kolmeya:
        db.salvar_metricas(dados_kolmeya, centro_custo, periodo_inicio, periodo_fim)
    
    if dados_4net:
        db.salvar_metricas(dados_4net, centro_custo, periodo_inicio, periodo_fim)
    
    if dados_whatsapp:
        db.salvar_metricas(dados_whatsapp, centro_custo, periodo_inicio, periodo_fim)
    
    if dados_ad:
        db.salvar_metricas(dados_ad, centro_custo, periodo_inicio, periodo_fim)
    
    print("✅ Todas as métricas do dashboard foram salvas no banco de dados")

if __name__ == "__main__":
    # Teste do banco de dados
    db = DashboardDatabase()
    
    # Exemplo de dados de teste
    dados_teste = {
        'canal': 'Kolmeya',
        'sms_enviados': 1000,
        'interacoes': 15.5,
        'investimento': 80.0,
        'taxa_entrega': 95.2,
        'total_vendas': 25,
        'producao': 125000.0,
        'leads_gerados': 164,
        'ticket_medio': 5000.0,
        'roi': 124920.0
    }
    
    # Salvar dados de teste
    db.salvar_metricas(dados_teste, "FGTS", datetime.now(), datetime.now())
    
    # Consultar estatísticas
    stats = db.obter_estatisticas_gerais()
    print(f"📊 Estatísticas do banco: {stats}")
