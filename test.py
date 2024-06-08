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

    conn=get_db_connection()
    cur=conn.cursor()
    cur.execute("SELECT * FROM zip_codes_mx_municipality WHERE state_id = %s AND name = %s", (state_id, municipality_name))
    municipality = cur.fetchone()

    if municipality is None:
        cur.execute("INSERT INTO zip_codes_mx_municipality (state_id, name, search_vector) VALUES (%s, %s, to_tsvector(%s))",
                    (state_id, 
                    municipality_name, 
                    municipality_vector, 
                    ))
        conn.commit()
        cur.execute("SELECT * FROM zip_codes_mx_municipality WHERE state_id = %s AND name = %s", (state_id, municipality_name))
        municipality = cur.fetchone()
        print(f'Municipality {municipality[1]} created')
    else:
        print(f'Municipality {municipality[1]} already exists')
    
    return municipality


def create_city(cities_list, municipality_id):

    conn=get_db_connection()
    cur=conn.cursor()

    for city in cities_list:
        print(f"Creando la ciudad: {city}")
        city = str(city)
        city_vector = vector_build(city)
        cur.execute("SELECT * FROM zip_codes_mx_city WHERE municipality_id = %s AND name = %s", (municipality_id, city))
        city_cursor = cur.fetchone()
        if city_cursor is None:
            cur.execute("INSERT INTO zip_codes_mx_city (municipality_id, name, search_vector) VALUES (%s, %s, to_tsvector(%s))",
                        (municipality_id, 
                        city, 
                        city_vector, 
                        ))
            conn.commit()
            cur.execute("SELECT * FROM zip_codes_mx_city WHERE municipality_id = %s AND name = %s", (municipality_id, city))
            city_cursor = cur.fetchone()
            print(f'City {city_cursor[1]} created')
        else:
            print(f'City {city} already exists')


def create_postal_code(postal_code_list, municipality_id):

    conn=get_db_connection()
    cur=conn.cursor()

    for postal_code in postal_code_list:
        print(f"Creando el codigo postal: {postal_code}")
        postal_code = str(postal_code)
        cur.execute("SELECT * FROM zip_codes_mx_postalcode WHERE municipality_id = %s AND code = %s", (municipality_id, postal_code))
        postal_code_cursor = cur.fetchone()
        if postal_code_cursor is None:
            cur.execute("INSERT INTO zip_codes_mx_postalcode (municipality_id, code) VALUES (%s, %s)",
                        (municipality_id, 
                        postal_code, 
                        ))
            conn.commit()
            cur.execute("SELECT * FROM zip_codes_mx_postalcode WHERE municipality_id = %s AND code = %s", (municipality_id, postal_code))
            postal_code_cursor = cur.fetchone()
            print(f'Postal code {postal_code_cursor[1]} created')
        else:
            print(f'Postal code {postal_code_cursor[1]} already exists')




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

    print(f"municipality_postalcode_dict: {municipality_postalcode_dict}")

    # GUARDAR EL ESTADO CREADO O EXISTENTE
    state_on_bd = create_state(state_name, entity_number)

    for municipality in municipalities:
        municipality_on_bd = create_municipality(municipality, state_on_bd[0])
        
        # VERIFICAR SI EL MUNICIPIO TIENE CIUDADES EN EL DICT
        municipalities_cities = municipality_city_dict.get(municipality)
        municipalities_postalcodes = municipality_postalcode_dict.get(municipality)

        # CREAR CIUDADES Y CODIGOS POSTALES
        if municipalities_cities:
            create_city(municipalities_cities, municipality_on_bd[0])
        else:
            print(f'No cities found for {municipality}')
        
        if municipalities_postalcodes:
            create_postal_code(municipalities_postalcodes, municipality_on_bd[0])
        else:
            print(f'No postal codes found for {municipality}')



    #print(municipality_city_dict)
    #print(municipality_postalcode_dict)


create_zip_code_records(zelda, 18)