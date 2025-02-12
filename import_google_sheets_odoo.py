import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

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
        print("📥 Chargement du fichier CSV...")
        df = pd.read_csv(csv_file, delimiter='\t')  # Délimiteur tabulation
    except Exception as e:
        return f"❌ Erreur lors du chargement du fichier CSV : {str(e)}"
    
    expected_columns = [
        "Kunden-Nr", "Artikel-Nr.", "Herstellerartikelnummer", "Artikelbezeichnung in FR",
        "UVP exkl. MwSt.", "Nettopreis exkl. MwSt.", "Brand", "EAN-Code"
    ]
    if not all(col in df.columns for col in expected_columns):
        return "❌ Le fichier CSV ne contient pas toutes les colonnes attendues."
    
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
            float(row.get("UVP exkl. MwSt.", 0)),
            float(row.get("Nettopreis exkl. MwSt.", 0)),
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
