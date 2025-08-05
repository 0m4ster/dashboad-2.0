#!/bin/bash
# Configurações para melhorar a estabilidade
export PYTHONUNBUFFERED=1
export STREAMLIT_SERVER_PORT=${PORT:-8501}
export STREAMLIT_SERVER_ADDRESS=0.0.0.0
export STREAMLIT_SERVER_HEADLESS=true
export STREAMLIT_SERVER_ENABLE_CORS=false
export STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false

# Inicia o Streamlit com configurações otimizadas
streamlit run api_kolm.py \
    --server.port $STREAMLIT_SERVER_PORT \
    --server.address $STREAMLIT_SERVER_ADDRESS \
    --server.headless $STREAMLIT_SERVER_HEADLESS \
    --server.enableCORS $STREAMLIT_SERVER_ENABLE_CORS \
    --server.enableXsrfProtection $STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION \
    --server.maxUploadSize 200 \
    --server.maxMessageSize 200 