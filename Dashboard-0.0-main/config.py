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

# Removendo credenciais FACTA e a função get_facta_users()
# FACTA_USUARIO = os.getenv('FACTA_USUARIO', '')
# FACTA_SENHA = os.getenv('FACTA_SENHA', '')

def get_facta_users():
    """Retorna lista de usuários FACTA configurados (agora vazia, pois não há credenciais)"""
    return []

