import os
import pandas as pd
import psycopg2
import csv
import sys
import xmlrpc.client
from psycopg2.extras import execute_values
from datetime import datetime

# 🔹 Augmenter la limite de lecture des lignes CSV
csv.field_size_limit(sys.maxsize)

# 🔹 Chemin du dossier où le fichier est uploadé
UPLOAD_FOLDER = '/var/www/webroot/ROOT/uploads/'

# 🔹 Connexion à Odoo avec JSON-RPC et clé API
ODOO_URL = "https://alex-mecanique.odoo.com/"
ODOO_DB = "alex-mecanique"
ODOO_API_KEY = os.getenv("ODOO_API_KEY")  # Utilisation de la variable d'environnement
ODOO_USERNAME = "pascal@925.ch"  # Remplace avec ton email Odoo

if not ODOO_API_KEY:
    print("❌ Clé API Odoo non définie. Vérifie ta variable d'environnement ODOO_API_KEY.")
    sys.exit(1)

common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})

if not uid:
    print("❌ Erreur d'authentification à Odoo. Vérifie ton email et ta clé API.")
    sys.exit(1)

odoo = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')

# 🔹 Taille des batchs pour l'import
BATCH_SIZE = 100

def get_existing_products():
    existing_products = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'search_read', [[]], {'fields': ['default_code', 'id']})
    return {p['default_code']: p['id'] for p in existing_products if p['default_code']}

def create_or_update_products(product_batch):
    existing_products = get_existing_products()
    new_products = []
    update_products = []
    
    for product_data in product_batch:
        default_code = product_data['default_code']
        if default_code in existing_products:
            update_products.append((existing_products[default_code], product_data))
        else:
            new_products.append(product_data)
    
    if new_products:
        created_ids = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'create', [new_products])
        print(f"✅ {len(created_ids)} nouveaux produits importés.")
    
    if update_products:
        for prod_id, data in update_products:
            odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'write', [[prod_id], data])
        print(f"🔄 {len(update_products)} produits mis à jour.")

def process_uploaded_file():
    csv_file = os.path.join(UPLOAD_FOLDER, "Derendinger - PF-9208336.csv")
    if not os.path.exists(csv_file):
        return f"❌ Fichier non trouvé : {csv_file}"
    return process_csv(csv_file)

def process_csv(csv_file):
    try:
        print("📥 Chargement du fichier CSV avec encodage UTF-8...")
        df_iterator = pd.read_csv(csv_file, delimiter=',', encoding='utf-8', quoting=csv.QUOTE_MINIMAL, on_bad_lines='skip', dtype=str, chunksize=10000)
        df = pd.concat(df_iterator, ignore_index=True)
        df.columns = df.columns.str.strip()
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    print("🔄 Début de l'importation dans Odoo...")
    product_batch = []
    
    for _, row in df.iterrows():
        product_data = {
            'name': row.get("Nom", ""),
            'list_price': float(row.get("Prix de vente", "0")),
            'standard_price': float(row.get("Fournisseurs / Prix", "0")),
            'barcode': row.get("Code-barres", ""),
            'default_code': row.get("Fournisseurs / Code du produit du fournisseur", ""),
        }
        product_batch.append(product_data)
        
        if len(product_batch) >= BATCH_SIZE:
            create_or_update_products(product_batch)
            product_batch = []
    
    if product_batch:
        create_or_update_products(product_batch)
    
    return "✅ Importation des produits terminée."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    print(process_uploaded_file())
