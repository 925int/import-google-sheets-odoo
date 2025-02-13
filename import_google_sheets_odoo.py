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

def create_or_update_product(product_data):
    # Vérifier si le code-barres existe déjà
    if product_data['barcode']:
        existing_barcode = odoo.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'product.template', 'search', [[['barcode', '=', product_data['barcode']]]])
        if existing_barcode:
            print(f"⚠️ Code-barres déjà existant. Importation du produit sans code-barres : {product_data['name']}")
            product_data['barcode'] = ""  # Supprimer le code-barres avant l'importation
    
    # Vérifier si le produit existe déjà via default_code
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
        print("📥 Chargement du fichier CSV avec correction d'encodage et séparateur...")
        df_iterator = pd.read_csv(csv_file, delimiter=',', encoding='utf-8', quoting=csv.QUOTE_MINIMAL, on_bad_lines='skip', dtype=str, chunksize=10000)
        df = pd.concat(df_iterator, ignore_index=True)
        df.columns = df.columns.str.strip()

        # Supprimer les lignes où "Artikelbezeichnung in FR" est vide
        df = df[df["Artikelbezeichnung in FR"].notna() & df["Artikelbezeichnung in FR"].str.strip().ne("")]

        # Vérification du nombre de lignes chargées
        print(f"🔍 Nombre de lignes chargées dans df: {len(df)}")

        # Convertir les prix en float en remplaçant les virgules par des points
        df["UVP exkl. MwSt."] = df["UVP exkl. MwSt."].astype(str).str.replace(',', '.').astype(float)
        df["Nettopreis exkl. MwSt."] = df["Nettopreis exkl. MwSt."].astype(str).str.replace(',', '.').astype(float)

        # Correction du format des codes EAN pour éviter la notation scientifique
        df["EAN-Code"] = df["EAN-Code"].apply(lambda x: f"{int(float(x))}" if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x)

        # Nettoyage des espaces dans "Artikel-Nr."
        df["Artikel-Nr."] = df["Artikel-Nr."].str.replace(" ", "")

        # Création de la colonne "Fournisseurs / ID externe" avec préfixe "drd."
        df["Fournisseurs / ID externe"] = "drd." + df["Artikel-Nr."]

        # Remplacement des valeurs "9208336" par "Derendinger AG" dans "Kunden-Nr"
        df["Kunden-Nr"] = df["Kunden-Nr"].replace("9208336", "Derendinger AG")
        
        # Ajout de la colonne de mise à jour
        df["date_mise_a_jour"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df["maj_odoo"] = "Non"
        
        # Renommer les colonnes
        df.rename(columns={
            "Kunden-Nr": "Fournisseurs / Fournisseur",
            "Artikel-Nr.": "Fournisseurs / Code du produit du fournisseur",
            "Artikelbezeichnung in FR": "Nom",
            "UVP exkl. MwSt.": "Prix de vente",
            "Nettopreis exkl. MwSt.": "Fournisseurs / Prix",
            "EAN-Code": "Code-barres"
        }, inplace=True)
        
        # Dupliquer "Fournisseurs / Prix" sous "Standard_Price"
        df["Standard_Price"] = df["Fournisseurs / Prix"]
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    print("🔄 Début de l'importation dans Odoo...")
    
    for _, row in df.iterrows():
        product_data = {
            'name': row.get("Nom", ""),
            'list_price': float(row.get("Prix de vente", "0")),
            'standard_price': float(row.get("Standard_Price", "0")),
            'barcode': row.get("Code-barres", ""),
            'default_code': row.get("Fournisseurs / Code du produit du fournisseur", ""),
        }
        create_or_update_product(product_data)
    
    return "✅ Importation des produits dans Odoo terminée."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    print(process_uploaded_file())
