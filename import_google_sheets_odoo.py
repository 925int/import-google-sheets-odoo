import os
import pandas as pd
import psycopg2
import csv
import sys
from psycopg2.extras import execute_values

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
        df.columns = df.columns.str.strip()  # Normaliser les noms de colonnes

        # Convertir les prix en float en remplaçant les virgules par des points
        df["UVP exkl. MwSt."] = df["UVP exkl. MwSt."].astype(str).str.replace(',', '.').astype(float)
        df["Nettopreis exkl. MwSt."] = df["Nettopreis exkl. MwSt."].astype(str).str.replace(',', '.').astype(float)

        # Correction du format des codes EAN pour éviter la notation scientifique
        df["EAN-Code"] = df["EAN-Code"].apply(lambda x: f"{int(float(x))}" if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x)
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    expected_columns = [
        "Kunden-Nr", "Artikel-Nr.", "Herstellerartikelnummer", "Artikelbezeichnung in FR",
        "UVP exkl. MwSt.", "Nettopreis exkl. MwSt.", "Brand", "EAN-Code"
    ]
    found_columns = df.columns.tolist()
    
    if not all(col in found_columns for col in expected_columns):
        missing_columns = [col for col in expected_columns if col not in found_columns]
        extra_columns = [col for col in found_columns if col not in expected_columns]
        print("❌ Colonnes attendues mais manquantes :", missing_columns)
        print("⚠️ Colonnes trouvées en trop :", extra_columns)
        return "❌ Le fichier CSV ne contient pas toutes les colonnes attendues. Vérifiez les noms et formats."
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produits (
            id SERIAL PRIMARY KEY,
            kunden_nr VARCHAR(255),
            artikel_nr VARCHAR(255),
            herstellerartikelnummer VARCHAR(255),
            artikelbezeichnung_fr VARCHAR(255),
            uvp_exkl_mwst NUMERIC(10,2),
            nettopreis_exkl_mwst NUMERIC(10,2),
            brand VARCHAR(255),
            ean_code VARCHAR(255)
        )
    """)
    conn.commit()
    
    insert_query = """
        INSERT INTO produits (kunden_nr, artikel_nr, herstellerartikelnummer, artikelbezeichnung_fr, 
                              uvp_exkl_mwst, nettopreis_exkl_mwst, brand, ean_code) 
        VALUES %s
    """
    data_to_insert = [
        (
            row.get("Kunden-Nr", ""),
            row.get("Artikel-Nr.", ""),
            row.get("Herstellerartikelnummer", ""),
            row.get("Artikelbezeichnung in FR", ""),
            float(row.get("UVP exkl. MwSt.", 0) or 0),
            float(row.get("Nettopreis exkl. MwSt.", 0) or 0),
            row.get("Brand", ""),
            row.get("EAN-Code", "")
        )
        for _, row in df.iterrows()
    ]
    execute_values(cursor, insert_query, data_to_insert)
    conn.commit()
    
    cursor.close()
    conn.close()
    
    return f"✅ {len(data_to_insert)} produits insérés dans PostgreSQL."

if __name__ == '__main__':
    print("📂 Vérification des fichiers uploadés...")
    print(process_uploaded_file())
