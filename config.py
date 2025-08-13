import os
from typing import Optional

class Config:
    """Configurações do sistema para ambiente local e nuvem."""
    
    # Verificar se está rodando no Render
    IS_RENDER = os.getenv('RENDER', False)
    
    # Configurações do banco de dados
    if IS_RENDER:
        # Render - PostgreSQL
        DATABASE_URL = os.getenv('DATABASE_URL')
        DATABASE_TYPE = 'postgresql'
        print("🌐 Executando no Render - Usando PostgreSQL")
    else:
        # Local - SQLite
        DATABASE_URL = "dashboard.db"
        DATABASE_TYPE = 'sqlite'
        print("🏠 Executando localmente - Usando SQLite")
    
    # Configurações da aplicação
    APP_NAME = "Dashboard Kolmeya"
    APP_VERSION = "1.0"
    
    # Configurações de retenção de dados
    RETENCAO_DIAS = int(os.getenv('RETENCAO_DIAS', '90'))
    
    # Configurações de exportação
    EXPORT_FORMATS = ['csv', 'excel']
    
    @classmethod
    def get_database_config(cls) -> dict:
        """Retorna configurações do banco de dados."""
        return {
            'type': cls.DATABASE_TYPE,
            'url': cls.DATABASE_URL,
            'is_render': cls.IS_RENDER
        }
    
    @classmethod
    def is_production(cls) -> bool:
        """Verifica se está em produção (Render)."""
        return cls.IS_RENDER
    
    @classmethod
    def get_app_info(cls) -> dict:
        """Retorna informações da aplicação."""
        return {
            'name': cls.APP_NAME,
            'version': cls.APP_VERSION,
            'environment': 'production' if cls.IS_RENDER else 'development',
            'database_type': cls.DATABASE_TYPE
        }
