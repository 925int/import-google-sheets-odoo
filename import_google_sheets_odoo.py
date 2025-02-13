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

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def get_tag_id(tag_name):
    tag = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.tag', 'search_read', [[['name', '=', tag_name]]], {'fields': ['id']})
    if tag:
        return tag[0]['id']
    else:
        print(f"🟢 Création du tag : {tag_name}")
        return odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.tag', 'create', [{'name': tag_name}])

def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            id_externe TEXT UNIQUE,
            default_code TEXT UNIQUE,
            product_name TEXT,
            list_price FLOAT,
            standard_price FLOAT,
            product_tag INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Table 'products' vérifiée et mise à jour si nécessaire.")

def insert_into_postgres(product_data):
    if not product_data:
        print("⚠️ Aucune donnée à insérer dans PostgreSQL.")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        execute_values(cursor, '''
            INSERT INTO products (id_externe, default_code, product_name, list_price, standard_price, product_tag, last_updated)
            VALUES %s
            ON CONFLICT (default_code) DO NOTHING 
            SET list_price = EXCLUDED.list_price,
                standard_price = EXCLUDED.standard_price,
                product_tag = EXCLUDED.product_tag,
                last_updated = NOW()
        ''', product_data)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM products;")
        row_count = cursor.fetchone()[0]
        print(f"✅ Nombre de lignes en base après insertion : {row_count}")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion dans PostgreSQL : {e}")
    finally:
        cursor.close()
        conn.close()

def create_products_in_odoo():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id_externe, default_code, product_name, list_price, standard_price, product_tag FROM products")
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    
    for product in products:
        product_data = {
            'default_code': product[1],
            'name': product[2],
            'list_price': product[3],
            'standard_price': product[4],
            'product_tag_ids': [(6, 0, [product[5]])]
        }
        print(f"🟢 Tentative de création/mise à jour dans Odoo : {product_data}")
        existing_product = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'search_read', [[['default_code', '=', product_data['default_code']]]], {'fields': ['id']})
        
        if existing_product:
            product_id = existing_product[0]['id']
            odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'write', [[product_id], product_data])
            print(f"🔄 Produit mis à jour : {product_data['name']}")
        else:
            odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'create', [product_data])
            print(f"✅ Nouveau produit importé : {product_data['name']}")

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
        print(df.head(5))  # Vérifier si les données sont bien lues
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    print("🔄 Début de l'importation dans PostgreSQL...")
    product_data_list = []
    tag_id = get_tag_id("Derendinger")
    
    for _, row in df.iterrows():
        artikel_nr = str(row.get("Artikel-Nr.", "")).strip()
        if not artikel_nr:
            continue
        try:
            product_data = (
                f"drd.{artikel_nr}",
                artikel_nr,
                row.get("Artikelbezeichnung in FR", ""),
                float(row.get("UVP exkl. MwSt.", "0") or 0),
                float(row.get("Nettopreis exkl. MwSt.", "0") or 0),
                tag_id,
                datetime.now()
            )
            product_data_list.append(product_data)
        except ValueError as e:
            print(f"⚠️ Erreur de conversion sur une ligne ignorée : {e}")
            continue
    
    print(f"🟢 Nombre d'articles à insérer dans PostgreSQL : {len(product_data_list)}")
    insert_into_postgres(product_data_list)
    return "✅ Importation des produits terminée."

if __name__ == '__main__':
    create_table()
    print(process_uploaded_file())
    create_products_in_odoo()
