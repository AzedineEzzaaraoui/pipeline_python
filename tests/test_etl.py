import sys
import os
import pandas as pd
from datetime import datetime
import pytest

# Ajouter le répertoire parent au chemin Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importer les fonctions depuis proce.py
from proce import extraced_excel, drop_missing_values, drop_duplicate_values, process_excel

# Test pour drop_missing_values
def test_drop_missing_values():
    data = {'col1': [1, None, 3], 'col2': [4, 5, None]}
    df = pd.DataFrame(data)
    df_cleaned = drop_missing_values(df)
    assert df_cleaned.shape == (1, 2)

# Test pour drop_duplicate_values
def test_drop_duplicate_values():
    data = {'col1': [1, 1, 2], 'col2': [4, 4, 5]}
    df = pd.DataFrame(data)
    df_cleaned = drop_duplicate_values(df)
    assert df_cleaned.shape == (2, 2)

# Test pour process_excel
def test_process_excel():
    data = {
        'Date': ['2023-01-01'],
        'Unit price': [10],
        'Product line': ['A'],
        'Customer type': ['B'],
        'gross margin percentage': [0.5],
        'gross income': [100]
    }
    df = pd.DataFrame(data)
    df_transformed = process_excel(df)
    assert 'date_chargement' in df_transformed.columns
    assert 'Year' in df_transformed.columns
    assert df_transformed['Year'].iloc[0] == 2023
    assert df_transformed['Month'].iloc[0] == 1
    assert df_transformed['day'].iloc[0] == 1
    assert 'Unit_price' in df_transformed.columns
    assert 'Product' in df_transformed.columns
    assert 'Customer_type' in df_transformed.columns
    assert 'gross_margin_percentage' in df_transformed.columns
    assert 'gross_income' in df_transformed.columns

# Point d'entrée pour exécuter les tests
if __name__ == "__main__":
    print("Début des tests...")
    pytest.main()
    print("Fin des tests.")