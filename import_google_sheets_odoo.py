import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
import os
import json

# 🔹 Récupération des credentials depuis la variable d'environnement
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not credentials_json:
    raise ValueError("Les credentials Google Cloud ne sont pas définis dans les variables d'environnement.")

creds_data = json.loads(credentials_json)
creds = Credentials.from_service_account_info(creds_data)
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
ODOO_API_KEY = os.getenv("ODOO_API_KEY")
if not ODOO_API_KEY:
    raise ValueError("La clé API Odoo n'est pas définie dans les variables d'environnement.")

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
