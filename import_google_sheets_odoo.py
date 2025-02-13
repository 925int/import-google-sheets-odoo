import os
import pandas as pd
import psycopg2
import csv
import sys
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

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

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
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produits (
            id SERIAL PRIMARY KEY,
            fournisseur VARCHAR(255),
            code_produit_fournisseur VARCHAR(255) UNIQUE,
            id_externe VARCHAR(255),
            herstellerartikelnummer VARCHAR(255),
            nom VARCHAR(255),
            prix_de_vente NUMERIC(10,2),
            fournisseurs_prix NUMERIC(10,2),
            standard_price NUMERIC(10,2),
            brand VARCHAR(255),
            code_barres VARCHAR(255),
            date_mise_a_jour TIMESTAMP,
            maj_odoo VARCHAR(10)
        )
    """)
    conn.commit()
    
    for _, row in df.iterrows():
        cursor.execute("SELECT * FROM produits WHERE code_produit_fournisseur = %s", (row["Fournisseurs / Code du produit du fournisseur"],))
        existing_product = cursor.fetchone()

        if existing_product:
            differences = [i for i, col in enumerate(df.columns) if str(existing_product[i]) != str(row[col])]
            if differences:
                cursor.execute("UPDATE produits SET date_mise_a_jour = %s, maj_odoo = 'Oui' WHERE code_produit_fournisseur = %s", (datetime.now(), row["Fournisseurs / Code du produit du fournisseur"]))
        else:
            cursor.execute("""
                INSERT INTO produits (fournisseur, code_produit_fournisseur, id_externe, herstellerartikelnummer, nom, 
                                      prix_de_vente, fournisseurs_prix, standard_price, brand, code_barres, date_mise_a_jour, maj_odoo) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Oui')
            """, (
                row.get("Fournisseurs / Fournisseur", ""),
                row.get("Fournisseurs / Code du produit du fournisseur", ""),
                row.get("Fournisseurs / ID externe", ""),
                row.get("Herstellerartikelnummer", ""),
                row.get("Nom", ""),
                float(row.get("Prix de vente", "0")),
                float(row.get("Fournisseurs / Prix", "0")),
                float(row.get("Standard_Price", "0")),
                row.get("Brand", ""),
                row.get("Code-barres", ""),
                datetime.now()
            ))
    conn.commit()
    cursor.close()
    conn.close()
    
    return "✅ Mise à jour et insertion des produits terminée."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    print(process_uploaded_file())
