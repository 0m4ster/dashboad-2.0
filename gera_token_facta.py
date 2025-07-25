import requests
import base64
import time

# Substitua pelos seus dados fornecidos pela Facta
usuario = "97832"
senha = "t8jmp66fyt2alr7v4e2b"

# Gera o valor base64 para o header Authorization
auth = f"{usuario}:{senha}"
auth_b64 = base64.b64encode(auth.encode()).decode()

headers = {
    "Authorization": f"Basic {auth_b64}"
}

url = "https://webservice.facta.com.br/gera-token"

while True:
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
        print("Status code:", resp.status_code)
        print("Resposta:", data)
        if not data.get("erro") and "token" in data:
            print("\nToken gerado com sucesso:")
            print(data["token"])
            with open("facta_token.txt", "w") as f:
                f.write(data["token"])
        else:
            print("\nNão foi possível gerar o token. Mensagem:", data.get("mensagem"))
    except Exception as e:
        print("Erro ao decodificar resposta:", e)
        print("Resposta bruta:", resp.text)
    print("Aguardando 15 minutos para gerar novo token...\n")
    time.sleep(900)  # 900 segundos = 15 minutos 