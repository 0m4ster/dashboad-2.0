import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações da API Kolmeya
KOLMEYA_API_BASE_URL = os.getenv('KOLMEYA_API_BASE_URL', 'https://kolmeya.com.br/api/v1/sms/reports/statuses')
KOLMEYA_API_ACCESSES_URL = os.getenv('KOLMEYA_API_ACCESSES_URL', 'https://kolmeya.com.br/api/v1/sms/accesses')
KOLMEYA_DEFAULT_TOKEN = os.getenv('KOLMEYA_DEFAULT_TOKEN', '')

# Configurações da API FACTA
FACTA_API_URLS = {
    'homologacao': os.getenv('FACTA_API_URL_HOMOLOGACAO', 'https://webservice-homol.facta.com.br/proposta/andamento-propostas'),
    'producao': os.getenv('FACTA_API_URL_PRODUCAO', 'https://webservice.facta.com.br/proposta/andamento-propostas')
}
FACTA_DEFAULT_TOKEN = os.getenv('FACTA_DEFAULT_TOKEN', '')

# Credenciais FACTA para geração de tokens
FACTA_USUARIO = os.getenv('FACTA_USUARIO', '')
FACTA_SENHA = os.getenv('FACTA_SENHA', '')

# Configurações do Streamlit
STREAMLIT_SERVER_PORT = int(os.getenv('STREAMLIT_SERVER_PORT', 8501))
STREAMLIT_SERVER_ADDRESS = os.getenv('STREAMLIT_SERVER_ADDRESS', '0.0.0.0')

def get_environment_info():
    """Retorna informações sobre o ambiente atual"""
    return {
        'kolmeya_token_set': bool(KOLMEYA_DEFAULT_TOKEN),
        'facta_token_set': bool(FACTA_DEFAULT_TOKEN),
        'facta_credentials_set': bool(FACTA_USUARIO and FACTA_SENHA),
        'environment': os.getenv('ENVIRONMENT', 'development')
    }

