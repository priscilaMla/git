import requests # Para fazer as requisições HTTP (login, lista, download).
import os       # Para lidar com caminhos de arquivos e criar pastas.
import json     # Para salvar e carregar o histórico de IDs (JSON).
from bs4 import BeautifulSoup # Para analisar o HTML do formulário de login (Keycloak).
from datetime import datetime # Para funções futuras (opcional, mas bom ter).

# --- CONFIGURAÇÕES DE ARQUIVOS E PASTAS ---
# Nome do arquivo onde salvaremos os IDs das notas já baixadas
HISTORY_FILE = "notas_baixadas.json" 
# Nome da pasta onde os arquivos .zip serão salvos
DOWNLOAD_FOLDER = "NotasFiscais"

BASE_URL = "https://app.qive.com.br"

INITIAL_LOGIN_URL = "https://auth.qive.com.br/auth/realms/saas/protocol/openid-connect/auth?approval_prompt=force&client_id=saas&redirect_uri=https%3A%2F%2Fapp.qive.com.br%2Foauth2%2Fcallback&response_type=code&scope=openid+profile+email&state=QUALQUER_STATE"

LIST_URL = f"{BASE_URL}/375797?filter%5BemissionDateStart=2000-01-01" 

DOWNLOAD_URL_PATTERN = f"{BASE_URL}/NFeS-{{invoice_id}}.zip"

YOUR_USERNAME = "compras@pecista.com.br"
YOUR_PASSWORD = "@Compras2025"


# --- Funções Auxiliares (Gerenciamento de Histórico) ---

def load_downloaded_ids(filename=HISTORY_FILE):
    """
    Carrega o conjunto de IDs de notas fiscais já baixadas do arquivo JSON.
    Se o arquivo não existir, retorna um conjunto vazio (set()).
    """
    if not os.path.exists(filename):
        return set()
    try:
        with open(filename, 'r') as f:
            # Carrega a lista do JSON e converte para 'set' (conjunto) para buscas rápidas
            return set(json.load(f))
    except (IOError, json.JSONDecodeError):
        print("Aviso: Não foi possível ler o arquivo de histórico. Criando novo.")
        return set()

def save_downloaded_ids(ids_set, filename=HISTORY_FILE):
    """
    Salva o conjunto atualizado de IDs de notas fiscais baixadas no arquivo JSON.
    """
    try:
        with open(filename, 'w') as f:
            # Converte o 'set' (conjunto) de volta para uma lista para salvar no JSON
            json.dump(list(ids_set), f, indent=4)
        print(f"\nHistórico salvo em: {filename}")
    except IOError as e:
        print(f"Erro ao salvar o histórico: {e}")


def get_invoice_list(session):
    """
    Busca a lista de todas as notas fiscais disponíveis no LIST_URL.
    
    ⚠️ Você PRECISA ajustar o 'data.get('items', [])' e os campos 'id' e 'date' 
    com base na estrutura exata do JSON do seu site.
    """
    print(f"\nBuscando lista de notas em: {LIST_URL}")
    
    # Parâmetros para garantir que você pegue todas as notas (limite de 5000)
    params = {
        "page": 1,
        "limit": 5000, 
    }
    
    try:
        response = session.get(LIST_URL, params=params)
        response.raise_for_status() # Lança erro se o status for 4xx ou 5xx
        data = response.json()
        
        # ⚠️ AJUSTE AQUI: Onde a lista de notas está no JSON.
        # Exemplo: se o JSON for {"total": 50, "invoices": [...]}, use 'invoices'
        all_items = data.get('items', []) # Tentativa: O JSON pode usar a chave 'items'
        
        invoice_list = []
        for item in all_items:
            # ⚠️ AJUSTE AQUI: Qual é o campo que contém o ID da nota (a chave que vai no NFeS-ID.zip)?
            invoice_id = str(item.get('chave_nota') or item.get('id_nf'))
            
            # Adicione a nota se o ID for válido
            if invoice_id:
                invoice_list.append({
                    'id': invoice_id,
                    'date': item.get('data_emissao', '1970-01-01').split('T')[0], # Pega só a data
                })
        
        print(f"✅ Encontradas {len(invoice_list)} notas na plataforma.")
        return invoice_list
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar lista de notas: {e}")
        return []
    except (json.JSONDecodeError, KeyError) as e:
        print(f"❌ Erro ao analisar o JSON de retorno. Verifique a chave 'items' e 'id'. Erro: {e}")
        return []