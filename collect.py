import pandas as pd
import unicodedata
from utils import get_db_connection

def vector_build(word):
    if type(word) is not str:
        word = str(word)
    word = word.lower().replace(" ", "_")
    word = ''.join((c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn'))
    word = ''.join(c for c in word if c.isalnum() or c == "_")
    return word

def create_zip_code_records(file_url, sheet_name):
    data = pd.read_excel(file_url, sheet_name)
    
    state_name = data['d_estado'].unique()[0]
    municipalities = data['D_mnpio'].unique()
    cities = data['d_ciudad'].dropna().unique()
    postal_codes = data['d_codigo'].unique()

    conn = get_db_connection()
    cur = conn.cursor()

    # Insertar Estado
    cur.execute("SELECT entity_number FROM zip_codes_mx_state WHERE entity_number = %s", (sheet_name,))
    state = cur.fetchone()
    if state is None:
        vector_name_state = vector_build(state_name)
        cur.execute("INSERT INTO zip_codes_mx_state (entity_number, name, search_vector) VALUES (%s, %s, to_tsvector(%s))",
                    (sheet_name, state_name, vector_name_state))
        conn.commit()
        print(f'State {sheet_name} created')
    else:
        print(f'State {sheet_name} already exists')

    # Insertar Municipios
    for municipality_name in municipalities:
        municipality_vector_name = vector_build(municipality_name)
        cur.execute("SELECT id FROM zip_codes_mx_municipality WHERE state_id = %s AND name = %s", (sheet_name, municipality_name))
        municipality = cur.fetchone()
        if municipality is None:
            cur.execute("INSERT INTO zip_codes_mx_municipality (state_id, name, search_vector) VALUES (%s, %s, to_tsvector(%s))",
                        (sheet_name, municipality_name, municipality_vector_name))
            conn.commit()
            print(f'Municipality {municipality_name} created')

    # Insertar Ciudades
    for city_name in cities:
        city_name_vector = vector_build(city_name)
        for _, row in data[data['d_ciudad'] == city_name].iterrows():
            municipality_name = row['D_mnpio']
            municipality_vector_name = vector_build(municipality_name)
            cur.execute("SELECT id FROM zip_codes_mx_municipality WHERE state_id = %s AND search_vector = %s", 
                        (sheet_name, municipality_vector_name))
            municipality = cur.fetchone()
            if municipality:
                cur.execute("SELECT id FROM zip_codes_mx_city WHERE municipality_id = %s AND name = %s", 
                            (municipality[0], city_name))
                city = cur.fetchone()
                if city is None:
                    cur.execute("INSERT INTO zip_codes_mx_city (municipality_id, name, search_vector) VALUES (%s, %s, to_tsvector(%s))",
                                (municipality[0], city_name, city_name_vector))
                    conn.commit()
                    print(f'City {city_name} created')

    # Insertar Códigos Postales y Asentamientos
    for _, row in data.iterrows():
        code = str(row['d_codigo']).zfill(5) if sheet_name == 9 else str(row['d_codigo'])
        city_name = row['d_ciudad'] if pd.notna(row['d_ciudad']) else row['D_mnpio']
        municipality_name = row['D_mnpio']
        
        cur.execute("SELECT id FROM zip_codes_mx_city WHERE name = %s", (city_name,))
        city = cur.fetchone()
        if city:
            cur.execute("SELECT id FROM zip_codes_mx_postalcode WHERE city_id = %s AND code = %s", (city[0], code))
            postal_code = cur.fetchone()
            if postal_code is None:
                cur.execute("INSERT INTO zip_codes_mx_postalcode (city_id, code) VALUES (%s, %s)", (city[0], code))
                conn.commit()
                cur.execute("SELECT id FROM zip_codes_mx_postalcode WHERE city_id = %s AND code = %s", (city[0], code))
                postal_code = cur.fetchone()
                print(f'PostalCode {code} created')
            
            settlement_name = row['d_asenta']
            settlement_name_vector = vector_build(settlement_name)
            settlement_type = row['d_tipo_asenta']
            cur.execute("SELECT id FROM zip_codes_mx_settlement WHERE postal_code_id = %s AND name = %s AND settlement_type = %s",
                        (postal_code[0], settlement_name, settlement_type))
            settlement = cur.fetchone()
            if settlement is None:
                cur.execute("INSERT INTO zip_codes_mx_settlement (postal_code_id, name, settlement_type, search_vector) VALUES (%s, %s, %s, to_tsvector(%s))",
                            (postal_code[0], settlement_name, settlement_type, settlement_name_vector))
                conn.commit()
                print(f'Settlement {settlement_name} created')

    cur.close()
    conn.close()

def collect_data(url):
    for index in range(4, 5):
        create_zip_code_records(url, index)
