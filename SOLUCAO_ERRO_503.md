# Solução para Erro 503 - Service Unavailable

## Problema Identificado
O erro 503 indica que o servidor está temporariamente indisponível ou sobrecarregado. Isso foi causado por:

1. **Conflitos de merge** no arquivo `api_kolm.py`
2. **Código duplicado** e inconsistente
3. **Configurações inadequadas** do Streamlit

## Soluções Implementadas

### 1. Correção dos Conflitos de Merge
- Criado script `fix_merge_conflicts.py` para corrigir automaticamente os conflitos
- Todos os marcadores `<<<<<<< HEAD`, `=======`, e `>>>>>>> commit_hash` foram removidos

### 2. Versão Limpa do Dashboard
- Criado `api_kolm_clean.py` com código otimizado e sem conflitos
- Removido código duplicado
- Melhorado tratamento de erros

### 3. Configurações Otimizadas

#### Arquivo `.streamlit/config.toml`
```toml
[server]
headless = true
enableCORS = false
enableXsrfProtection = false
port = 8501

[browser]
gatherUsageStats = false

[theme]
base = "dark"
```

#### Arquivo `start.sh` atualizado
- Configurações de estabilidade adicionadas
- Timeouts otimizados
- Limites de upload configurados

#### Arquivo `render.yaml` melhorado
- Variáveis de ambiente adicionadas
- Health check configurado
- Auto-deploy habilitado

## Como Usar

### Opção 1: Usar a versão limpa (Recomendado)
```bash
# O start.sh já está configurado para usar api_kolm_clean.py
streamlit run api_kolm_clean.py
```

### Opção 2: Corrigir o arquivo original
```bash
# Execute o script de correção
python fix_merge_conflicts.py

# Depois use o arquivo original
streamlit run api_kolm.py
```

## Melhorias Implementadas

1. **Tratamento de Erros Robusto**
   - Try/catch em todas as chamadas de API
   - Fallbacks para dados ausentes
   - Mensagens de erro informativas

2. **Otimizações de Performance**
   - Cache de dados com TTL
   - Timeouts configurados
   - Garbage collection automático

3. **Configurações de Estabilidade**
   - Headless mode habilitado
   - CORS desabilitado
   - XSRF protection desabilitado
   - Limites de upload reduzidos

## Variáveis de Ambiente Necessárias

Certifique-se de que as seguintes variáveis estão configuradas:
- `KOLMEYA_TOKEN`: Token de autenticação do Kolmeya
- `ARGUS_TOKEN`: Token de autenticação da URA (Argus)
- `FACTA_TOKEN`: Token de autenticação da Facta (opcional)

## Deploy no Render

O arquivo `render.yaml` já está configurado com:
- Health check path: `/`
- Auto-deploy habilitado
- Variáveis de ambiente configuradas
- Plano starter selecionado

## Monitoramento

Para monitorar o status do dashboard:
1. Acesse o painel do Render
2. Verifique os logs em tempo real
3. Monitore o health check endpoint

## Próximos Passos

1. **Teste localmente** primeiro:
   ```bash
   streamlit run api_kolm_clean.py
   ```

2. **Faça commit das mudanças**:
   ```bash
   git add .
   git commit -m "Correção de conflitos de merge e otimizações"
   git push
   ```

3. **Monitore o deploy** no Render

4. **Verifique se o erro 503 foi resolvido**

## Contato

Se o problema persistir, verifique:
- Logs do Render
- Status das APIs externas (Kolmeya, Argus)
- Configuração das variáveis de ambiente 