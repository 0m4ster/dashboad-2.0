from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

# O endereço do seu robô na rede local
ROBO_URL = "http://192.168.0.245:5000/total?token=meu_token_secreto"

# Token de segurança para a API intermediária
# É importante que o Render use este token ao chamar esta API
API_TOKEN = "seu_token_super_secreto_para_a_ponte" 

@app.route('/dados_robo', methods=['GET'])
def get_dados_robo():
    # Verifica se o token de autorização foi enviado
    auth_token = request.headers.get('Authorization')
    if not auth_token or auth_token != f"Bearer {API_TOKEN}":
        return jsonify({"erro": "Token de autorização inválido ou ausente"}), 401

    try:
        # Chama o robô na rede local
        resp = requests.get(ROBO_URL, timeout=5)
        resp.raise_for_status()
        dados = resp.json()
        return jsonify(dados)
    except requests.exceptions.RequestException as e:
        # Retorna um erro se não conseguir conectar ao robô
        return jsonify({"erro": f"Não foi possível conectar ao robô: {str(e)}"}), 503
    except Exception as e:
        return jsonify({"erro": f"Ocorreu um erro inesperado: {str(e)}"}), 500

if __name__ == '__main__':
    # Roda o servidor na porta 5001, acessível na sua rede
    app.run(host='0.0.0.0', port=5001) 