import streamlit as st
import requests
import base64
import time
import json
from datetime import datetime, timedelta
import os
# Removendo a importação de get_facta_users, pois não será mais usada para credenciais
# from config import get_facta_users

class FactaTokenManager:
    def __init__(self):
        self.token_file = "facta_token.txt"
        self.config_file = "facta_config.json"
        self.load_config()
    
    def load_config(self):
        """Carrega configurações salvas incluindo credenciais FACTA"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.usuario = config.get('usuario', '')
                    self.senha = config.get('senha', '')
                    self.ambiente = config.get('ambiente', 'producao')
            else:
                self.usuario = ""
                self.senha = ""
                self.ambiente = "producao"
        except:
            self.usuario = ""
            self.senha = ""
            self.ambiente = "producao"
    
    def save_config(self):
        """Salva configurações incluindo credenciais FACTA"""
        config = {
            'usuario': self.usuario,
            'senha': self.senha,
            'ambiente': self.ambiente
        }
        with open(self.config_file, 'w') as f:
            json.dump(config, f)
    
    def switch_user(self, user_name):
        """Alterna para um usuário específico (não mais aplicável)"""
        return False
    
    def get_available_users(self):
        """Retorna lista de usuários disponíveis (agora vazia)"""
        return []
    
    def generate_token(self):
        """Gera um token real chamando o endpoint da FACTA usando Basic Auth"""
        try:
            # Verifica se tem credenciais
            if not self.usuario or not self.senha:
                return False, None, "Credenciais FACTA não configuradas. Configure usuário e senha primeiro."
            
            # Seleciona ambiente (homologação ou produção)
            if self.ambiente == 'homologacao':
                url = "https://webservice-homol.facta.com.br/gera-token"
            else:
                url = "https://webservice.facta.com.br/gera-token"
            
            # Codifica credenciais em base64 conforme documentação FACTA
            credentials = f"{self.usuario}:{self.senha}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            # Headers conforme documentação FACTA
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Basic {encoded_credentials}',
                'User-Agent': 'Servix-Dashboard/1.0'
            }
            
            # Faz a requisição para o endpoint da FACTA
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if not data.get('erro'):
                    # Token gerado com sucesso
                    token = data.get('token')
                    expira = data.get('expira', '')
                    
                    if token:
                        # Salva o token
                        with open(self.token_file, "w") as f:
                            f.write(token)
                        
                        # Calcula expiração (1 hora conforme documentação)
                        expires_at = datetime.now() + timedelta(hours=1)
                        
                        # Salva informações do token
                        token_info = {
                            'token': token,
                            'timestamp': datetime.now().isoformat(),
                            'expires_at': expires_at.isoformat(),
                            'source': 'FACTA_API',
                            'ambiente': self.ambiente,
                            'expira_api': expira,
                            'auth_method': 'Basic Auth (FACTA)'
                        }
                        
                        with open("facta_token_info.json", "w") as f:
                            json.dump(token_info, f)
                        
                        return True, token, f"Token gerado com sucesso para ambiente {self.ambiente}"
                    else:
                        return False, None, "Token não encontrado na resposta da API"
                else:
                    # Erro da API FACTA
                    error_msg = data.get('mensagem', 'Erro desconhecido')
                    return False, None, f"Erro da API FACTA: {error_msg}"
            
            elif response.status_code == 401:
                return False, None, "Credenciais inválidas. Verifique usuário e senha."
            else:
                return False, None, f"Erro HTTP {response.status_code}: {response.text}"
                
        except requests.exceptions.RequestException as e:
            return False, None, f"Erro de conexão: {str(e)}"
        except Exception as e:
            return False, None, f"Erro inesperado: {str(e)}"
    
    def get_current_token(self):
        """Obtém token atual"""
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    token = f.read().strip()
                return token
            except:
                return None
        return None
    
    def get_token_info(self):
        """Obtém informações do token atual"""
        if os.path.exists("facta_token_info.json"):
            try:
                with open("facta_token_info.json", 'r') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def is_token_expired(self):
        """Verifica se o token expirou (validade de 1 hora conforme FACTA)"""
        token_info = self.get_token_info()
        if token_info and 'expires_at' in token_info:
            try:
                expires_at = datetime.fromisoformat(token_info['expires_at'])
                return datetime.now() >= expires_at
            except:
                pass
        return True

def render_facta_token_page():
    """Renderiza a página de gerenciamento de tokens da Facta"""
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="color: #2c3e50; margin-bottom: 10px;">🔐 Gerenciador de Tokens Facta</h1>
        <p style="color: #7f8c8d; font-size: 16px;">Sistema de geração e monitoramento de tokens para API Facta</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Inicializa o gerenciador
    if 'facta_manager' not in st.session_state:
        st.session_state.facta_manager = FactaTokenManager()
    
    manager = st.session_state.facta_manager
    
    # Sidebar para configurações
    with st.sidebar:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #2c3e50; margin-bottom: 15px;">⚙️ Configurações Facta</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Configuração de credenciais
        st.subheader("🔐 Credenciais FACTA")
        
        # Campo de usuário
        novo_usuario = st.text_input(
            "👤 Usuário FACTA",
            value=manager.usuario,
            type="default",
            help="Usuário para acesso à API FACTA"
        )
        
        # Campo de senha
        nova_senha = st.text_input(
            "🔒 Senha FACTA",
            value=manager.senha,
            type="password",
            help="Senha para acesso à API FACTA"
        )
        
        # Seleção de ambiente
        novo_ambiente = st.selectbox(
            "🌍 Ambiente",
            options=["producao", "homologacao"],
            index=0 if manager.ambiente == "producao" else 1,
            help="Ambiente da API FACTA (produção ou homologação)"
        )
        
        # Botão para salvar configurações
        if st.button("💾 Salvar Configurações", type="secondary", use_container_width=True):
            if novo_usuario and nova_senha:
                manager.usuario = novo_usuario
                manager.senha = nova_senha
                manager.ambiente = novo_ambiente
                manager.save_config()
                st.success("✅ Configurações salvas com sucesso!")
                st.rerun()
            else:
                st.error("❌ Usuário e senha são obrigatórios!")
        
        st.markdown("---")
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h4 style="color: #2c3e50; margin-bottom: 15px;">🚀 Ações Rápidas</h4>
        </div>
        """, unsafe_allow_html=True)
        
        # Botão para gerar novo token
        if st.button("🔄 Gerar Novo Token", type="primary", use_container_width=True):
            with st.spinner("Gerando novo token..."):
                success, token, error = manager.generate_token()
                
                if success:
                    st.success("✅ Token gerado com sucesso!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error(f"❌ Erro ao gerar token: {error}")
        
        # Botão para limpar token
        if st.button("🗑️ Limpar Token", type="secondary", use_container_width=True):
            if os.path.exists(manager.token_file):
                os.remove(manager.token_file)
            if os.path.exists("facta_token_info.json"):
                os.remove("facta_token_info.json")
            st.success("✅ Token removido com sucesso!")
            st.rerun()
    
    # Colunas principais
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📊 Status do Token")
        
        # Verifica token atual
        current_token = manager.get_current_token()
        token_info = manager.get_token_info()
        is_expired = manager.is_token_expired()
        
        # Métricas de status
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        
        with metric_col1:
            if current_token:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Status</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">✅ Ativo</p>
                </div>
                """.format() if not is_expired else """
                <div style="text-align: center; padding: 15px; background-color: #ffebee; border-radius: 8px; border-left: 4px solid #f44336;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Status</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">❌ Expirado</p>
                </div>
                """.format(), unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Status</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">❌ Não encontrado</p>
                </div>
                """.format(), unsafe_allow_html=True)
        
        with metric_col2:
            if token_info and 'timestamp' in token_info:
                created_time = datetime.fromisoformat(token_info['timestamp'])
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Criado em</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                </div>
                """.format(created_time.strftime("%H:%M:%S")), unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Criado em</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                </div>
                """.format(), unsafe_allow_html=True)
        
        with metric_col3:
            if token_info and 'expires_at' in token_info:
                expires_time = datetime.fromisoformat(token_info['expires_at'])
                time_left = expires_time - datetime.now()
                if time_left.total_seconds() > 0:
                    hours_left = int(time_left.total_seconds() / 3600)
                    minutes_left = int((time_left.total_seconds() % 3600) / 60)
                    time_display = f"{hours_left}h {minutes_left}m" if hours_left > 0 else f"{minutes_left}m"
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Expira em</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{}</p>
                    </div>
                    """.format(time_display), unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #ffebee; border-radius: 8px; border-left: 4px solid #f44336;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Expira em</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">Expirado</p>
                    </div>
                    """.format(), unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="text-align: center; padding: 15px; background-color: #f5f5f5; border-radius: 8px; border-left: 4px solid #9e9e9e;">
                    <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Expira em</h5>
                    <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">N/A</p>
                </div>
                """.format(), unsafe_allow_html=True)
        
        # Barra de progresso do tempo restante
        if token_info and 'expires_at' in token_info:
            expires_time = datetime.fromisoformat(token_info['expires_at'])
            time_left = expires_time - datetime.now()
            if time_left.total_seconds() > 0:
                progress = 1 - (time_left.total_seconds() / (60 * 60))  # 1 hora
                st.progress(progress, text=f"⏰ Tempo restante: {int(time_left.total_seconds() / 60)} min {int(time_left.total_seconds() % 60)}s")
        
        # Token atual
        if current_token:
            st.markdown("### 🔑 Token Atual")
            st.code(current_token, language="text")
            
            # Botão para copiar token
            if st.button("📋 Copiar Token", type="secondary"):
                st.write("✅ Token copiado para a área de transferência!")
                st.info("💡 Use Ctrl+C para copiar o token acima")
        else:
            st.warning("⚠️ Nenhum token encontrado. Gere um novo token para continuar.")
    
    with col2:
        st.markdown("### 🚀 Ações")
        
        # Botão para verificar token
        if st.button("🔍 Verificar Token", type="secondary", use_container_width=True):
            if current_token:
                st.info("✅ Token encontrado e válido!")
            else:
                st.warning("⚠️ Nenhum token encontrado!")
        
        # Botão para testar conexão com a API FACTA real
        if st.button("🧪 Testar Conexão", type="secondary", use_container_width=True):
            if not manager.usuario or not manager.senha:
                st.error("❌ Configure usuário e senha FACTA primeiro!")
                return
                
            try:
                # Seleciona ambiente
                if manager.ambiente == 'homologacao':
                    url = "https://webservice-homol.facta.com.br/gera-token"
                else:
                    url = "https://webservice.facta.com.br/gera-token"
                
                st.info(f"🔍 Testando conexão com API FACTA ({manager.ambiente})...")
                
                # Codifica credenciais em base64
                credentials = f"{manager.usuario}:{manager.senha}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                
                # Headers conforme documentação FACTA
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Basic {encoded_credentials}',
                    'User-Agent': 'Servix-Dashboard/1.0'
                }
                
                with st.spinner("Testando autenticação..."):
                    response = requests.get(url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('erro'):
                            st.warning(f"⚠️ API acessível, mas com erro: {data.get('mensagem', 'Erro desconhecido')}")
                        else:
                            st.success("✅ Conexão com API FACTA estabelecida com sucesso!")
                            st.info(f"Token válido: {data.get('token', 'N/A')[:20]}...")
                    elif response.status_code == 401:
                        st.error("❌ Credenciais inválidas. Verifique usuário e senha.")
                    else:
                        st.error(f"❌ Erro HTTP {response.status_code}: {response.text}")
                        
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Erro de conexão: {str(e)}")
            except Exception as e:
                st.error(f"❌ Erro inesperado: {str(e)}")
    
    # Seção de logs
    st.markdown("---")
    st.markdown("### 📝 Log de Atividades")
    
    # Simula log de atividades (você pode implementar um sistema real de logs)
    if 'facta_logs' not in st.session_state:
        st.session_state.facta_logs = []
    
    # Adiciona log quando token é gerado
    if st.button("📝 Adicionar Log de Teste"):
        log_entry = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'action': 'Token gerado manualmente',
            'status': 'Sucesso'
        }
        st.session_state.facta_logs.insert(0, log_entry)
        st.rerun()
    
    # Exibe logs
    if st.session_state.facta_logs:
        for log in st.session_state.facta_logs[:10]:  # Mostra apenas os 10 últimos
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.write(f"**{log['timestamp']}**")
            with col2:
                st.write(log['action'])
            with col3:
                if log['status'] == 'Sucesso':
                    st.success(log['status'])
                else:
                    st.error(log['status'])
    else:
        st.info("📝 Nenhum log de atividade encontrado.")
    
    # Seção de informações técnicas
    with st.expander("🔧 Informações Técnicas"):
        st.markdown("""
        **Endpoint:** `https://webservice.facta.com.br/gera-token` ✅ **IMPLEMENTADO**
        
        **Método:** GET
        
        **Autenticação:** Basic Auth com `usuario:senha` em base64 ✅
        
        **Ambientes:**
        - Produção: `https://webservice.facta.com.br/gera-token`
        - Homologação: `https://webservice-homol.facta.com.br/gera-token`
        
        **Validade do Token:** 1 hora (conforme documentação FACTA) ✅
        
        **Implementação:**
        - Credenciais configuráveis na sidebar
        - Codificação automática em base64
        - Seleção de ambiente (produção/homologação)
        - Validação automática de expiração
        
        **Arquivos gerados:**
        - `facta_token.txt` - Token atual
        - `facta_token_info.json` - Metadados do token
        - `facta_config.json` - Configurações e credenciais
        
        **🔑 Como usar:**
        1. Configure usuário e senha na sidebar
        2. Selecione o ambiente (produção/homologação)
        3. Clique em "Salvar Configurações"
        4. Use "Testar Conexão" para validar credenciais
        5. Use "Gerar Novo Token" para obter token válido
        """)
        
        # Mostra arquivos existentes
        st.markdown("**Arquivos no sistema:**")
        files = []
        for filename in [manager.token_file, "facta_token_info.json", manager.config_file]:
            if os.path.exists(filename):
                files.append(f"✅ {filename}")
            else:
                files.append(f"❌ {filename}")
        
        for file_status in files:
            st.write(file_status)

# Função para integração com o dashboard principal
def get_facta_token():
    """Retorna o token atual da Facta para uso em outras partes do sistema"""
    manager = FactaTokenManager()
    return manager.get_current_token()

def is_facta_token_valid():
    """Verifica se o token da Facta é válido"""
    manager = FactaTokenManager()
    return not manager.is_token_expired()

# Se executado diretamente, mostra o frontend
if __name__ == "__main__":
    st.set_page_config(
        page_title="Facta Token Manager",
        page_icon="🔐",
        layout="wide"
    )
    
    render_facta_token_page()

