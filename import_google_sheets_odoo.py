import os
import pandas as pd
import psycopg2
from flask import Flask, request, render_template_string
from psycopg2.extras import execute_values

# 🔹 Configuration de l'application Flask
app = Flask(__name__)
UPLOAD_FOLDER = '/var/www/webroot/ROOT/uploads/'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

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

# 🔹 Interface HTML pour uploader un fichier
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Uploader un fichier CSV</title>
</head>
<body>
    <h2>Importer un fichier CSV</h2>
    <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file" required>
        <input type="submit" value="Uploader">
    </form>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "❌ Aucun fichier sélectionné."
    
    file = request.files['file']
    if file.filename == '':
        return "❌ Aucun fichier sélectionné."
    
    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)
    
    # 🔹 Charger et importer les données dans PostgreSQL
    return process_csv(filepath)

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
    app.run(host='128.65.197.180', port=80, debug=True)
