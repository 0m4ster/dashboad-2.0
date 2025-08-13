# 📊 Dashboard Kolmeya + Banco de Dados

Sistema completo de dashboard para monitoramento de campanhas SMS, WhatsApp e AD, com armazenamento em banco de dados SQLite para análise histórica.

## 🚀 Funcionalidades

### Dashboard Principal (`api_kolm.py`)
- **Integração com APIs**: Kolmeya (SMS), Facta (propostas)
- **Processamento de dados**: Upload de arquivos CSV/Excel
- **Métricas em tempo real**: SMS enviados, interações, investimento, ROI
- **Múltiplos canais**: Kolmeya, 4NET, WhatsApp, AD
- **Centros de custo**: Novo, FGTS, CLT, Outros

### Banco de Dados (`database_manager.py`)
- **SQLite local**: Armazenamento automático de métricas
- **Tabelas principais**:
  - `metricas_dashboard`: Métricas por canal e período
  - `consultas_facta`: Histórico de consultas à API Facta
  - `configuracoes`: Configurações do sistema
- **Retenção automática**: Limpeza de dados antigos (90 dias)

### Analytics (`dashboard_analytics.py`)
- **Visualização histórica**: Gráficos e tabelas por período
- **Filtros avançados**: Canal, centro de custo, período
- **Exportação**: CSV e Excel
- **Gráficos interativos**: Plotly para análise visual

## 📋 Pré-requisitos

- Python 3.8+
- Acesso às APIs Kolmeya e Facta
- Arquivos de base de dados (CSV/Excel)

## 🛠️ Instalação

1. **Clone ou baixe os arquivos**
2. **Instale as dependências**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure as credenciais das APIs** (se necessário)

## 🚀 Como Usar

### 1. Dashboard Principal
```bash
streamlit run api_kolm.py
```
- Upload de arquivo de base
- Seleção de centro de custo
- Visualização de métricas em tempo real
- **As métricas são salvas automaticamente no banco**

### 2. Analytics e Histórico
```bash
streamlit run dashboard_analytics.py
```
- Visualização de dados históricos
- Filtros por período e canal
- Gráficos e análises
- Exportação de dados

### 3. Gerenciamento do Banco
```python
from database_manager import DashboardDatabase

# Inicializar banco
db = DashboardDatabase()

# Salvar métricas manualmente
db.salvar_metricas(dados, centro_custo, data_inicio, data_fim)

# Consultar histórico
metricas = db.obter_metricas_por_periodo('Kolmeya', data_inicio, data_fim)
```

## 🗄️ Estrutura do Banco de Dados

### Tabela `metricas_dashboard`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | INTEGER | Chave primária |
| `canal` | TEXT | Canal (Kolmeya, 4NET, WhatsApp, AD) |
| `sms_enviados` | INTEGER | Total de SMS enviados |
| `interacoes` | REAL | Taxa de interação (%) |
| `investimento` | REAL | Valor investido (R$) |
| `taxa_entrega` | REAL | Taxa de entrega (%) |
| `total_vendas` | INTEGER | Número de vendas |
| `producao` | REAL | Valor total produzido (R$) |
| `leads_gerados` | INTEGER | Total de leads |
| `ticket_medio` | REAL | Ticket médio (R$) |
| `roi` | REAL | Retorno sobre investimento (R$) |
| `data_coleta` | TIMESTAMP | Data/hora da coleta |
| `centro_custo` | TEXT | Centro de custo |
| `periodo_inicio` | DATE | Início do período |
| `periodo_fim` | DATE | Fim do período |

### Tabela `consultas_facta`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | INTEGER | Chave primária |
| `canal` | TEXT | Canal da consulta |
| `cpfs_consultados` | INTEGER | CPFs consultados |
| `propostas_encontradas` | INTEGER | Propostas encontradas |
| `valor_total_propostas` | REAL | Valor total (R$) |
| `taxa_conversao` | REAL | Taxa de conversão (%) |
| `data_consulta` | TIMESTAMP | Data da consulta |

## 📊 Métricas Coletadas

### Por Canal
- **Kolmeya**: SMS, interações, investimento, ROI
- **4NET**: URA, leads, vendas, produção
- **WhatsApp**: Campanhas, engajamento, conversões
- **AD**: Ações, efetividade, vendas

### Por Centro de Custo
- **Novo**: Campanhas de novos clientes
- **FGTS**: Campanhas FGTS
- **CLT**: Campanhas CLT
- **Outros**: Demais campanhas

## 🔧 Configurações

### Retenção de Dados
- **Padrão**: 90 dias
- **Configurável**: Via tabela `configuracoes`
- **Limpeza automática**: Via função `limpar_dados_antigos()`

### Exportação
- **Formatos**: CSV, Excel
- **Filtros**: Período, canal, centro de custo
- **Arquivos**: Timestamp automático

## 📈 Análises Disponíveis

### Gráficos
- Produção por canal
- ROI por canal
- Investimento por canal
- Evolução temporal

### Relatórios
- Métricas detalhadas por período
- Comparativo entre canais
- Histórico de consultas Facta
- Estatísticas do banco

## 🚨 Troubleshooting

### Erro de Importação
```bash
pip install -r requirements.txt
```

### Banco não encontrado
- Verifique se `database_manager.py` está na mesma pasta
- O banco é criado automaticamente na primeira execução

### Dados não aparecem
- Verifique se o dashboard principal foi executado
- Confirme se as métricas foram salvas no banco
- Use o botão "🔄 Atualizar Dados"

## 🔄 Atualizações

### Versão 1.0
- Dashboard principal funcional
- Sistema de banco de dados SQLite
- Interface de analytics
- Exportação de dados
- Limpeza automática

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs do console
2. Confirme as dependências instaladas
3. Teste com dados de exemplo

## 📝 Licença

Sistema desenvolvido para uso interno da empresa.

---

**Desenvolvido com ❤️ para otimizar campanhas de marketing digital**
