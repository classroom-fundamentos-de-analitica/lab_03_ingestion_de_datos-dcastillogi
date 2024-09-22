"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

"""
    Function to form the columns of the dataframe 
"""
def from_unclear_to_clear_columns(row, structure, is_header=False):
    parsed_row = []
    initial_position = 0
    last_position = structure[0]
    for i in range(len(structure)):
        col_content = row[initial_position:last_position].strip()
        if (col_content != ''):
            if (is_header):
                parsed_row.append('_' + col_content.strip().lower().replace(' ', '_'))
            else:
                parsed_row.append(' ' + col_content.strip())
            
        else:
            parsed_row.append('')

        if (last_position + 2 >= len(row)):
            break

        initial_position = last_position + 2
        
        # Last column complete with the rest of the row
        if (i >= len(structure) - 2):
            last_position = len(row)
        else:
            last_position += structure[i + 1] + 2
    
    return parsed_row

def ingest_data():

    # Read the file
    with open('clusters_report.txt') as report:
        rows = report.readlines()

    headers = []
    headers_structure = []


    pattern_to_separete = r'(\S.*?)(?=\s{2}\w|$)' # "space+space+word+space+space" pattern
    # Define the project structure
    h = rows.pop(0).strip()
    for header in re.findall(pattern_to_separete, h):
        he = header.strip().lower().replace(' ', '_')
        headers.append(he)
        headers_structure.append(len(header))

    # Complete the headers
    while True:
        r = rows.pop(0)
        if (r.strip() == ''):
            continue
        if (r.startswith('-')):
            break
        for i, col in enumerate(from_unclear_to_clear_columns(r, headers_structure, True)):
            headers[i] += col

    # Fill the dataframe
    data = [[]]
    for row in rows:
        if (row.strip() == ''):
            data.append([])
            continue
        for i, col in enumerate(from_unclear_to_clear_columns(row, headers_structure)):
            if (len(data[-1]) <= i):
                data[-1].append(col)
            else:
                data[-1][i] += col

    while True:
        if (data[-1] == []):
            data.pop()
        else:
            break
            
    # Add the data to the dataframe
    df = pd.DataFrame(data, columns=headers)

    # Custom transformations
    # Change columns cluster, cantidad_de_palabras_clave and porcentaje_de_palabras_clave to int and float
    df['cluster'] = df['cluster'].astype(int)
    df['cantidad_de_palabras_clave'] = df['cantidad_de_palabras_clave'].astype(int)
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace('%', '').str.replace(',', '.').astype(float)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace(r'\s+', ' ', regex=True)

    return df

ingest_data()