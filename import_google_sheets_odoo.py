import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
import os

# 🔹 Configuration Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SERVICE_ACCOUNT_FILE = "credentials.json"

# Vérification de l'existence du fichier credentials.json
script_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(script_dir, SERVICE_ACCOUNT_FILE)

if not os.path.exists(credentials_path):
    raise FileNotFoundError(f"Le fichier {credentials_path} est introuvable. Assurez-vous qu'il est dans le même dossier que le script.")

# Authentification Google
creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
client = gspread.authorize(creds)

# 🔹 Demande à l'utilisateur quel fichier importer
sheet_id = input("Entrez l'ID du fichier Google Sheets à importer : ")
sheet_name = input("Entrez le nom de l'onglet à importer : ")

# Ouvre la feuille Google Sheets
spreadsheet = client.open_by_key(sheet_id)
worksheet = spreadsheet.worksheet(sheet_name)

data = worksheet.get_all_records()
df = pd.DataFrame(data)

# 🔹 Paramètres Odoo avec clé API
ODOO_URL = "https://alex-mecanique.odoo.com"
ODOO_API_KEY = input("f93c51b15f5ab9a1b7878d840f74fc5cc6a90c53")
ODOO_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ODOO_API_KEY}"
}

# 🔹 Import des produits dans Odoo
for index, row in df.iterrows():
    product_data = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "product.template",
            "method": "create",
            "args": [{
                "name": row.get("Nom du produit", "Produit sans nom"),
                "default_code": row.get("Référence", ""),
                "list_price": row.get("Prix", 0),
                "categ_id": 1,  # Remplace par l’ID de la catégorie
            }],
            "kwargs": {},
        },
    }
    response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=product_data, headers=ODOO_HEADERS)
    
    if response.status_code == 200:
        print(f"✅ Produit ajouté : {row.get('Nom du produit', 'Produit sans nom')}")
    else:
        print(f"❌ Erreur sur {row.get('Nom du produit', 'Produit sans nom')} : {response.text}")
