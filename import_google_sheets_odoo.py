import pandas as pd
import psycopg2
import os

try:
    import micropip
except ModuleNotFoundError:
    print("⚠️ Module 'micropip' non trouvé. Assurez-vous que l'environnement supporte l'installation des dépendances.")

# 🔹 Connexion à PostgreSQL
POSTGRES_HOST = "node172643-env-8840643.jcloud.ik-server.com"
POSTGRES_DB = "alex_odoo"
POSTGRES_USER = "Odoo"
POSTGRES_PASSWORD = "C:2&#:4G9pAO823O@3iC"

try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()
except psycopg2.OperationalError as e:
    print(f"❌ Erreur de connexion à PostgreSQL : {e}")
    exit()

# 🔹 Création de la table si elle n'existe pas
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

# 🔹 Demander le chemin du fichier CSV à l'utilisateur
csv_file = input("Entrez le chemin du fichier CSV : ")

# 🔹 Charger le CSV en DataFrame
try:
    print("📥 Chargement du fichier CSV...")
    df = pd.read_csv(csv_file, delimiter='\t')  # Délimiteur tabulation
except FileNotFoundError:
    print(f"❌ Fichier non trouvé : {csv_file}")
    exit()
except pd.errors.EmptyDataError:
    print("❌ Le fichier CSV est vide.")
    exit()
except pd.errors.ParserError:
    print("❌ Erreur lors de la lecture du fichier CSV. Vérifiez le format.")
    exit()

# 🔹 Vérifier si le fichier contient les bonnes colonnes
expected_columns = [
    "Kunden-Nr", "Artikel-Nr.", "Herstellerartikelnummer", "Artikelbezeichnung in FR",
    "UVP exkl. MwSt.", "Nettopreis exkl. MwSt.", "Brand", "EAN-Code"
]
if not all(col in df.columns for col in expected_columns):
    print("❌ Le fichier CSV ne contient pas toutes les colonnes attendues.")
    print("Colonnes attendues :", expected_columns)
    print("Colonnes trouvées :", df.columns.tolist())
    exit()

# 🔹 Insérer les données dans PostgreSQL en bulk
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

from psycopg2.extras import execute_values
execute_values(cursor, insert_query, data_to_insert)
conn.commit()

print(f"✅ {len(data_to_insert)} produits insérés dans PostgreSQL.")

# 🔹 Fermeture de la connexion
cursor.close()
conn.close()
