import pandas as pd
from datetime import datetime
import pymysql

def extraced_excel(file_path):
    # Lire le fichier Excel
    df = pd.read_excel(file_path)

    # Supprimer les valeurs manquantes
    df_cleaned = drop_missing_values(df)
    print("Forme du DataFrame après suppression des valeurs manquantes :", df_cleaned.shape)

    # Supprimer les doublons
    df_cleaned = drop_duplicate_values(df_cleaned)
    print("Forme du DataFrame après suppression des doublons :", df_cleaned.shape)

    return df_cleaned

def drop_missing_values(df):
    # Supprimer les lignes avec des valeurs manquantes
    df_clean = df.dropna()
    return df_clean

def drop_duplicate_values(df):
    # Supprimer les lignes en double
    df_cleaned = df.drop_duplicates()
    return df_cleaned

def process_excel(df_extracted):
    # Ajouter une colonne 'date_chargement' avec la date et l'heure actuelles
    df_extracted['date_chargement'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convertir la colonne 'Date' en type datetime
    df_extracted['Date'] = pd.to_datetime(df_extracted['Date'])

    # Extraire l'année, le mois et le jour
    df_extracted['Year'] = df_extracted['Date'].dt.year
    df_extracted['Month'] = df_extracted['Date'].dt.month
    df_extracted['day'] = df_extracted['Date'].dt.day

    # Renommer les colonnes
    df_extracted = df_extracted.rename(columns={
        'Unit price': 'Unit_price',
        'Product line': 'Product',
        'Customer type': 'Customer_type',
        'gross margin percentage': 'gross_margin_percentage',
        'gross income': 'gross_income'
    })

    print("DataFrame après transformation :")
    print(df_extracted.head())

    # Retourner le DataFrame transformé
    return df_extracted

def load_to_mysql(df, db_config):
    try:
        # Connexion à la base de données MySQL
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database']
        )
        mycursor = conn.cursor()

        # Créer la table si elle n'existe pas
        create_table_query = """
        CREATE TABLE IF NOT EXISTS supermarket_sales (
            Invoice_ID VARCHAR(255),
            Branch VARCHAR(255),
            City VARCHAR(255),
            Customer_type VARCHAR(255),
            Gender VARCHAR(255),
            Product VARCHAR(255),
            Unit_price FLOAT,
            Quantity INT,
            Total FLOAT,
            Date DATETIME,
            gross_margin_percentage FLOAT,
            gross_income FLOAT,
            date_chargement DATETIME,
            Year INT,
            Month INT,
            day INT
        );
        """
        mycursor.execute(create_table_query)
        print("Table 'supermarket_sales' créée ou déjà existante.")

        # Insérer les données dans la table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO supermarket_sales (
                Invoice_ID, Branch, City, Customer_type, Gender, Product,
                Unit_price, Quantity, Total, Date, gross_margin_percentage,
                gross_income, date_chargement, Year, Month, day
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            mycursor.execute(insert_query, (
                row['Invoice ID'], row['Branch'], row['City'], row['Customer_type'],
                row['Gender'], row['Product'], row['Unit_price'], row['Quantity'],
                row['Total'], row['Date'], row['gross_margin_percentage'],
                row['gross_income'], row['date_chargement'], row['Year'],
                row['Month'], row['day']
            ))

        # Valider les changements
        conn.commit()
        print("Données chargées avec succès dans MySQL.")

    except pymysql.MySQLError as e:
        print(f"Erreur MySQL : {e}")
    finally:
        # Fermer la connexion
        if conn:
            conn.close()
            print("Connexion MySQL fermée.")

if __name__ == "__main__":
    # Chemin du fichier Excel
    file_path = r"C:\Users\user\my-python-project\online_retail\supermarket_sales.xlsx"

    # Configurations de la base de données MySQL
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'azedine',
        'database': 'datawarehouse'
    }

    # Extraire et nettoyer les données
    df_cleaned = extraced_excel(file_path)

    # Transformer les données
    df_transformed = process_excel(df_cleaned)

    # Charger les données dans MySQL
    load_to_mysql(df_transformed, db_config)