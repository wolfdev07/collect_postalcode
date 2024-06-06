import os
import pandas as pd
import unicodedata
from utils import get_db_connection

zelda = os.path.join('static', 'data', 'collection.xls')

def vector_build(word):
    if type(word) is not str:
        word = str(word)
    word = word.lower().replace(" ", "_")
    word = ''.join((c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn'))
    word = ''.join(c for c in word if c.isalnum() or c == "_")
    return word


def create_state(state_name, entity_number):
    state_name = str(state_name)
    state_vector = vector_build(state_name)
    print(state_vector)

    # CONECTAR A LA DB
    conn=get_db_connection()
    cur=conn.cursor()
    cur.execute("SELECT * FROM zip_codes_mx_state WHERE entity_number = %s AND name = %s", (entity_number, state_name))
    state = cur.fetchone()
    print(state)

    if state is None:
        cur.execute("INSERT INTO zip_codes_mx_state (entity_number, name, search_vector) VALUES (%s, %s, to_tsvector(%s))", 
                    (entity_number,
                    state_name,
                    state_vector))
        conn.commit()
        cur.execute("SELECT * FROM zip_codes_mx_state WHERE entity_number = %s AND name = %s", (entity_number, state_name))
        state = cur.fetchone()
        print(f'State {state[1]} created')
    else:
        print(f'State {state[1]} already exists')
    
    return state


def create_municipality(municipality_name, state_id):
    municipality_name = str(municipality_name)
    municipality_vector = vector_build(municipality_name)

    municipality_data = {
        'name': municipality_name,
        'search_vector': municipality_vector,
        'state_id': state_id
    }
    print("MUNICIPIOS")
    print(municipality_data)







def create_zip_code_records(url, entity_number):
    data = pd.read_excel(url, entity_number)

    state = data['d_estado'].unique()
    state_name = state[0]
    municipalities = data['D_mnpio'].unique()
    cities = data['d_ciudad'].dropna().unique()
    postal_codes = data['d_codigo'].unique()


    # ALMACENAR CIUDADES POR MUNICIPIO Y CODIGOS POSTALES
    municipality_city_dict = {}
    municipality_postalcode_dict = {}

    for municipality in municipalities:
        # Filtrar el DataFrame para las filas correspondientes a cada municipio
        filtered_df = data[data['D_mnpio'] == municipality]
        
        # Obtener los datos correspondientes, excluyendo NaN
        cities = filtered_df['d_ciudad'].dropna().unique().tolist()
        postal_codes = filtered_df['d_codigo'].unique().tolist()
        
        # Agregar la lista de ciudades al diccionario
        if cities:
            municipality_city_dict[municipality] = cities

        if postal_codes:
            municipality_postalcode_dict[municipality] = postal_codes
    
    state_on_bd = create_state(state_name, entity_number)

    for municipality in municipality_city_dict:
        create_municipality(municipality, state_on_bd[0])



    #print(municipality_city_dict)
    #print(municipality_postalcode_dict)


create_zip_code_records(zelda, 1)