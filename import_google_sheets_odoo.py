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

def get_db_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_import (
            id SERIAL PRIMARY KEY,
            product_code VARCHAR(255) UNIQUE NOT NULL,
            supplier_id INT NOT NULL,
            price FLOAT NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            default_code VARCHAR(255) UNIQUE NOT NULL,
            list_price FLOAT NOT NULL,
            standard_price FLOAT NOT NULL,
            barcode VARCHAR(255),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

def insert_into_postgres(product_data, supplier_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO product_import (product_code, supplier_id, price, last_updated)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (product_code) DO UPDATE 
        SET price = EXCLUDED.price, last_updated = NOW()
    ''', (supplier_data['product_code'], supplier_data['partner_id'], supplier_data['price']))

    cursor.execute('''
        INSERT INTO products (name, default_code, list_price, standard_price, barcode, last_updated)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (default_code) DO UPDATE 
        SET list_price = EXCLUDED.list_price, standard_price = EXCLUDED.standard_price, last_updated = NOW()
    ''', (product_data['name'], product_data['default_code'], product_data['list_price'], product_data['standard_price'], product_data['barcode']))
    
    conn.commit()
    cursor.close()
    conn.close()

def process_uploaded_file():
    csv_file = os.path.join(UPLOAD_FOLDER, "Derendinger - PF-9208336.csv")
    if not os.path.exists(csv_file):
        return f"❌ Fichier non trouvé : {csv_file}"
    return process_csv(csv_file)

def process_csv(csv_file):
    try:
        print("📥 Chargement du fichier CSV...")
        df = pd.read_csv(csv_file, delimiter=',', encoding='utf-8', quoting=csv.QUOTE_MINIMAL, on_bad_lines='skip', dtype=str)
        df.columns = df.columns.str.strip()
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    print("🔄 Début de l'importation dans PostgreSQL...")
    
    for _, row in df.iterrows():
        product_code = row.get("Fournisseurs / ID externe", "")
        if not product_code:
            continue  # Ignore les lignes sans product_code

        product_data = {
            'name': row.get("Nom", ""),
            'list_price': float(row.get("Prix de vente", "0")),
            'standard_price': float(row.get("Fournisseurs / Prix", "0")),
            'barcode': row.get("Code-barres", ""),
            'default_code': row.get("Fournisseurs / Code du produit du fournisseur", ""),
        }
        supplier_data = {
            'partner_id': get_supplier_id(),
            'product_code': product_code + ".four",
            'price': float(row.get("Fournisseurs / Prix", "0")),
        }
        insert_into_postgres(product_data, supplier_data)
    
    return "✅ Importation des produits dans PostgreSQL terminée."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    create_tables()
    print(process_uploaded_file())
