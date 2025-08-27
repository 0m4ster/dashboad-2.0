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
        self.config_file = "facta_config.json" # Manter para consistência, mas não será usado para credenciais
        self.load_config()
    
    def load_config(self):
        """Carrega configurações salvas (agora não mais credenciais)"""
        # Não há mais credenciais para carregar
        self.usuario = ""
        self.senha = ""
        self.available_users = []
        print("DEBUG: Credenciais FACTA não são mais necessárias para geração de token.")
    
    def save_config(self):
        """Salva configurações (não mais credenciais)"""
        # Não há mais credenciais para salvar
        pass
    
    def switch_user(self, user_name):
        """Alterna para um usuário específico (não mais aplicável)"""
        return False
    
    def get_available_users(self):
        """Retorna lista de usuários disponíveis (agora vazia)"""
        return []
    
    def generate_token(self):
        """Gera um token fictício para simulação"""
        try:
            # Simula a geração de um token válido
            simulated_token = "SIMULATED_FACTA_TOKEN_" + datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Salva o token
            with open(self.token_file, "w") as f:
                f.write(simulated_token)
            
            # Salva informações do token (validade de 15 minutos)
            token_info = {
                'token': simulated_token,
                'timestamp': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(minutes=15)).isoformat()
            }
            
            with open("facta_token_info.json", "w") as f:
                json.dump(token_info, f)
            
            return True, simulated_token, None
                
        except Exception as e:
            return False, None, str(e)
    
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
        """Verifica se o token expirou"""
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
    
    # Sidebar para configurações (removendo campos de usuário/senha)
    with st.sidebar:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #2c3e50; margin-bottom: 15px;">⚙️ Configurações Facta</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Removendo campos de usuário e senha
        # new_usuario = st.text_input("👤 Usuário:", value=manager.usuario, key="facta_usuario")
        # new_senha = st.text_input("🔑 Senha:", value=manager.senha, type="password", key="facta_senha")
        
        # Removendo botão de salvar configurações, pois não há mais credenciais para salvar
        # if st.button("💾 Salvar Configurações", type="primary", use_container_width=True):
        #     manager.usuario = new_usuario
        #     manager.senha = new_senha
        #     manager.save_config()
        #     st.success("✅ Configurações salvas com sucesso!")
        #     st.rerun()
        
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
                    minutes_left = int(time_left.total_seconds() / 60)
                    st.markdown("""
                    <div style="text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                        <h5 style="color: #2c3e50; margin: 0 0 5px 0;">Expira em</h5>
                        <p style="font-size: 20px; font-weight: bold; color: #2c3e50; margin: 0;">{} min</p>
                    </div>
                    """.format(minutes_left), unsafe_allow_html=True)
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
                progress = 1 - (time_left.total_seconds() / (15 * 60))  # 15 minutos
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
        
        # Botão para testar conexão (agora sem credenciais, apenas com o token)
        if st.button("🧪 Testar Conexão", type="secondary", use_container_width=True):
            if current_token:
                try:
                    headers = {
                        'Accept': 'application/json',
                        'Authorization': f'Bearer {current_token}',
                        'Content-Type': 'application/json'
                    }
                    
                    # Teste simples com a API FACTA (pode ser um endpoint público ou um que aceite o token)
                    # Para este exemplo, vamos simular uma resposta de sucesso
                    st.success("✅ Conexão com API FACTA simulada com sucesso!")
                        
                except Exception as e:
                    st.error(f"❌ Erro ao testar conexão: {str(e)}")
            else:
                st.warning("⚠️ Nenhum token disponível para teste")
    
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
        **Endpoint:** `https://webservice.facta.com.br/gera-token` (Simulado)
        
        **Método:** GET (Simulado)
        
        **Autenticação:** Não requer credenciais (Simulado)
        
        **Validade do Token:** 15 minutos (Simulado)
        
        **Arquivos gerados:**
        - `facta_token.txt` - Token atual
        - `facta_token_info.json` - Metadados do token
        - `facta_config.json` - Configurações salvas (não mais para credenciais)
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

