import os
import pandas as pd
import unicodedata
from utils import get_db_connection
from contextlib import contextmanager

# COLLECT DATA TO DB

def vector_build(word):
    if not isinstance(word, str):
        word = str(word)
    word = word.lower().replace(" ", "_")
    word = ''.join((c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn'))
    word = ''.join(c for c in word if c.isalnum() or c == "_")
    return word

@contextmanager
def get_cursor():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        yield cur
    finally:
        conn.commit()
        cur.close()
        conn.close()

def create_entity(table, unique_keys, values):
    with get_cursor() as cur:
        keys_str = ' AND '.join([f"{key} = %s" for key in unique_keys])
        cur.execute(f"SELECT * FROM {table} WHERE {keys_str}", tuple(unique_keys.values()))
        entity = cur.fetchone()

        if entity is None:
            columns = ', '.join(values.keys())
            placeholders = ', '.join(['%s'] * len(values))
            cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *", tuple(values.values()))
            entity = cur.fetchone()
            print(f'{table.capitalize()} {entity[1]} created')
        else:
            print(f'{table.capitalize()} {entity[1]} already exists')

        return entity

def create_state(state_name, entity_number):
    state_vector = vector_build(state_name)
    return create_entity(
        'zip_codes_mx_state', 
        {'entity_number': entity_number, 'name': state_name},
        {'entity_number': entity_number, 'name': state_name, 'search_vector': state_vector}
    )

def create_municipality(municipality_name, state_id):
    municipality_vector = vector_build(municipality_name)
    return create_entity(
        'zip_codes_mx_municipality', 
        {'state_id': state_id, 'name': municipality_name},
        {'state_id': state_id, 'name': municipality_name, 'search_vector': municipality_vector}
    )

def create_city(city_name, municipality_id):
    city_vector = vector_build(city_name)
    return create_entity(
        'zip_codes_mx_city', 
        {'municipality_id': municipality_id, 'name': city_name},
        {'municipality_id': municipality_id, 'name': city_name, 'search_vector': city_vector}
    )

def create_postal_code(postal_code, municipality_id):
    postal_code=str(postal_code)
    return create_entity(
        'zip_codes_mx_postalcode', 
        {'municipality_id': municipality_id, 'code': postal_code},
        {'municipality_id': municipality_id, 'code': postal_code}
    )

def create_settlement(settlement_name, settlement_type, postal_code_id):
    settlement_vector = vector_build(settlement_name)
    return create_entity(
        'zip_codes_mx_settlement', 
        {'postal_code_id': postal_code_id, 'name': settlement_name},
        {'postal_code_id': postal_code_id, 'name': settlement_name, 'settlement_type': settlement_type, 'search_vector': settlement_vector}
    )

def create_zip_code_records(url, entity_number):
    data = pd.read_excel(url, sheet_name=entity_number)

    state_name = data['d_estado'].unique()[0]
    municipalities = data['D_mnpio'].unique()
    postal_codes = data['d_codigo'].unique()

    state_on_bd = create_state(state_name, entity_number)

    municipality_city_dict = data.groupby('D_mnpio')['d_ciudad'].apply(lambda x: x.dropna().unique().tolist()).to_dict()
    municipality_postalcode_dict = data.groupby('D_mnpio')['d_codigo'].unique().to_dict()
    postalcode_settlement_dict = data.groupby('d_codigo')[['d_asenta', 'd_tipo_asenta']].apply(lambda x: x.drop_duplicates().to_records(index=False).tolist()).to_dict()

    for municipality in municipalities:
        municipality_on_bd = create_municipality(municipality, state_on_bd[0])
        
        municipalities_cities = municipality_city_dict.get(municipality)
        municipalities_postalcodes = municipality_postalcode_dict.get(municipality)

        if len(municipalities_cities)>0:
            for city in municipalities_cities:
                create_city(city, municipality_on_bd[0])

        if len(municipalities_postalcodes)>0:
            for postal_code in municipalities_postalcodes:
                postalcode_on_bd = create_postal_code(postal_code, municipality_on_bd[0])
                settlements_list = postalcode_settlement_dict.get(postalcode_on_bd[1], [])
                for settlement, settlement_type in settlements_list:
                    create_settlement(settlement, settlement_type, postalcode_on_bd[0])
