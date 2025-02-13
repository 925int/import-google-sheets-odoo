import os
import pandas as pd
import psycopg2
import csv
import sys
import requests
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

# 🔹 Connexion à Odoo avec API Key
ODOO_URL = "https://alex-mecanique.odoo.com/"
ODOO_DB = "alex-mecanique"
ODOO_API_KEY = os.getenv("ODOO_API_KEY")  # Utilisation de la variable d'environnement

if not ODOO_API_KEY:
    print("❌ Clé API Odoo non définie. Vérifie ta variable d'environnement ODOO_API_KEY.")
    sys.exit(1)

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def create_or_update_product(product_data):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {ODOO_API_KEY}'
    }
    search_url = f"{ODOO_URL}/api/product.template/search"
    update_url = f"{ODOO_URL}/api/product.template/update"
    create_url = f"{ODOO_URL}/api/product.template/create"

    # Vérifier si le produit existe déjà
    response = requests.post(search_url, json={'default_code': product_data['default_code']}, headers=headers)
    if response.status_code == 200 and response.json():
        product_id = response.json()[0]['id']
        update_response = requests.post(update_url, json={'id': product_id, **product_data}, headers=headers)
        if update_response.status_code == 200:
            print(f"🔄 Produit mis à jour : {product_data['name']}")
        else:
            print(f"❌ Erreur lors de la mise à jour du produit {product_data['name']}: {update_response.text}")
    else:
        create_response = requests.post(create_url, json=product_data, headers=headers)
        if create_response.status_code == 200:
            print(f"✅ Nouveau produit importé : {product_data['name']}")
        else:
            print(f"❌ Erreur lors de la création du produit {product_data['name']}: {create_response.text}")

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
