from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import time
from flask import Flask, jsonify, request
import threading

# Caminho para o geckodriver.exe
driver_path = r'C:\Users\Junin\Desktop\dash\kolmeya\chaves\geckodriver.exe'  # <-- Altere para o caminho correto

# Lista de meses para repetir o processo
meses = ['2025-07', '2025-06', '2025-05']  # Adicione os meses desejados

service = Service(driver_path)

email = "vandeirsantana@servcredrp.com.br"
senha = "Vandeir2025@"
url_login = "https://lemitti.com/auth/login"  
url_extrato = "https://lemitti.com/report/balance/2025-07"  # Coloque a URL real do extrato

# Token de autenticação simples
TOKEN = "meu_token_secreto"

# Variável global para armazenar o último valor lido
global_total = None

def fazer_login(driver):
    print("Acessando página de login...")
    driver.get(url_login)
    time.sleep(2)
    print("Preenchendo e-mail...")
    campo_email = driver.find_element(By.ID, "email")
    campo_email.clear()
    campo_email.send_keys(email)
    print("Preenchendo senha...")
    campo_senha = driver.find_element(By.ID, "password")
    campo_senha.clear()
    campo_senha.send_keys(senha)
    print("Clicando no botão de login...")
    botao_login = driver.find_element(By.XPATH, "//button[@type='submit']")
    botao_login.click()
    time.sleep(2)

def monitorar_total():
    global global_total
    while True:
        driver = None
        try:
            print("Iniciando navegador...")
            driver = webdriver.Firefox(service=service)
            fazer_login(driver)
            print("Acessando extrato...")
            driver.get(url_extrato)
            time.sleep(2)
            while True:
                try:
                    total_row = driver.find_element(By.CSS_SELECTOR, "tfoot tr.total-row")
                    total_ths = total_row.find_elements(By.TAG_NAME, "th")
                    total_values = [th.text for th in total_ths]
                    global_total = total_values
                    print(f"Total: {total_values}")
                    time.sleep(10)
                except NoSuchElementException:
                    print("Linha de total não encontrada, tentando novamente...")
                    time.sleep(5)
                except WebDriverException as wde:
                    print(f"WebDriverException detectada, reiniciando... {wde}")
                    break
        except Exception as e:
            print(f"Erro detectado: {e}. Reiniciando processo...")
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception as e:
                    print(f"Erro ao fechar o driver: {e}")
            time.sleep(5)

# Flask app para expor o endpoint
app = Flask(__name__)

@app.route('/total', methods=['GET'])
def get_total():
    if request.args.get("token") != TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    if global_total is not None:
        return jsonify({"total": global_total})
    else:
        return jsonify({"error": "Nenhum valor lido ainda"}), 404

if __name__ == '__main__':
    t = threading.Thread(target=monitorar_total, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=5000)
