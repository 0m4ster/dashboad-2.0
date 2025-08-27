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

# Credenciais FACTA para geração de tokens (fallback)
FACTA_USUARIO = os.getenv('FACTA_USUARIO', '')
FACTA_SENHA = os.getenv('FACTA_SENHA', '')



# Novas variáveis para múltiplos usuários
FACTA_EDU = os.getenv('FACTA_EDU', '')
FACTA_GENESIS = os.getenv('FACTA_GENESIS', '')
FACTA_HUGO = os.getenv('FACTA_HUGO', '')
FACTA_SENHA1 = os.getenv('FACTA_SENHA1', '')
FACTA_SENHA2 = os.getenv('FACTA_SENHA2', '')
FACTA_SENHA3 = os.getenv('FACTA_SENHA3', '')

# Configurações do Streamlit
STREAMLIT_SERVER_PORT = int(os.getenv('STREAMLIT_SERVER_PORT', 8501))
STREAMLIT_SERVER_ADDRESS = os.getenv('STREAMLIT_SERVER_ADDRESS', '0.0.0.0')

def get_environment_info():
    """Retorna informações sobre o ambiente atual"""
    return {
        'kolmeya_token_set': bool(KOLMEYA_DEFAULT_TOKEN),
        'facta_token_set': bool(FACTA_DEFAULT_TOKEN),
        'facta_credentials_set': bool(FACTA_USUARIO and FACTA_SENHA),
        'facta_multiple_users_set': bool(FACTA_EDU or FACTA_GENESIS or FACTA_HUGO),
        'environment': os.getenv('ENVIRONMENT', 'development')
    }

def get_facta_users():
    """Retorna lista de usuários FACTA configurados"""
    users = []
    
    if FACTA_EDU and FACTA_SENHA1:
        users.append({'name': 'EDU', 'usuario': FACTA_EDU, 'senha': FACTA_SENHA1})
    
    if FACTA_GENESIS and FACTA_SENHA2:
        users.append({'name': 'GENESIS', 'usuario': FACTA_GENESIS, 'senha': FACTA_SENHA2})
    
    if FACTA_HUGO and FACTA_SENHA3:
        users.append({'name': 'HUGO', 'usuario': FACTA_HUGO, 'senha': FACTA_SENHA3})
    
    # Fallback para variáveis antigas se as novas não estiverem configuradas
    if not users and FACTA_USUARIO and FACTA_SENHA:
        users.append({'name': 'DEFAULT', 'usuario': FACTA_USUARIO, 'senha': FACTA_SENHA})
    
    return users

