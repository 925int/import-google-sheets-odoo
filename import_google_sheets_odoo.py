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

# 🔹 Connexion à PostgreSQL
POSTGRES_HOST = "node172643-env-8840643.jcloud.ik-server.com"
POSTGRES_DB = "alex_odoo"
POSTGRES_USER = "Odoo"
POSTGRES_PASSWORD = "C:2&#:4G9pAO823O@3iC"

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

def get_supplier_id():
    supplier_name = "Derendinger AG"
    supplier = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'search_read', [[['name', '=', supplier_name]]], {'fields': ['id']})
    if supplier:
        return supplier[0]['id']
    else:
        return odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'create', [{'name': supplier_name, 'supplier_rank': 1}])

def create_or_update_product(product_data, supplier_data):
    # Vérifier si le code-barres existe déjà
    if product_data['barcode']:
        existing_barcode = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'search', [[['barcode', '=', product_data['barcode']]]])
        if existing_barcode:
            print(f"⚠️ Code-barres déjà existant. Importation du produit sans code-barres : {product_data['name']}")
            product_data['barcode'] = ""  # Supprimer le code-barres avant l'importation
    
    # Vérifier si le produit existe déjà via default_code
    existing_product = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'search_read', [[['default_code', '=', product_data['default_code']]]], {'fields': ['id', 'list_price', 'standard_price']})
    
    if existing_product:
        product_id = existing_product[0]['id']
        price_update = {
            'list_price': product_data['list_price'],
            'standard_price': product_data['standard_price']
        }
        odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'write', [[product_id], price_update])
        print(f"🔄 Prix mis à jour pour : {product_data['name']}")
    else:
        product_id = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'create', [product_data])
        print(f"✅ Nouveau produit importé : {product_data['name']}")
    
    # Ajouter le fournisseur
    supplier_data['product_tmpl_id'] = product_id
    supplier_data['partner_id'] = get_supplier_id()
    odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.supplierinfo', 'create', [supplier_data])
    print(f"✅ Fournisseur ajouté pour : {product_data['name']}")

def process_uploaded_file():
    csv_file = os.path.join(UPLOAD_FOLDER, "Derendinger - PF-9208336.csv")
    if not os.path.exists(csv_file):
        return f"❌ Fichier non trouvé : {csv_file}"
    return process_csv(csv_file)

def process_csv(csv_file):
    try:
        print("📥 Chargement du fichier CSV avec correction d'encodage et séparateur...")
        df_iterator = pd.read_csv(csv_file, delimiter=',', encoding='utf-8', quoting=csv.QUOTE_MINIMAL, on_bad_lines='skip', dtype=str, chunksize=10000)
        df = pd.concat(df_iterator, ignore_index=True)
        df.columns = df.columns.str.strip()
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    print("🔄 Début de l'importation dans Odoo...")
    
    for _, row in df.iterrows():
        product_data = {
            'name': row.get("Nom", ""),
            'list_price': float(row.get("Prix de vente", "0")),
            'standard_price': float(row.get("Fournisseurs / Prix", "0")),
            'barcode': row.get("Code-barres", ""),
            'default_code': row.get("Fournisseurs / Code du produit du fournisseur", ""),
        }
        supplier_data = {
            'partner_id': get_supplier_id(),
            'product_code': row.get("Fournisseurs / ID externe", ""),
            'price': float(row.get("Fournisseurs / Prix", "0")),
            'delay': 1,
        }
        create_or_update_product(product_data, supplier_data)
    
    return "✅ Importation des produits et fournisseurs dans Odoo terminée."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    print(process_uploaded_file())
