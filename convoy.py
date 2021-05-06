# Write your code here
import pandas as pd
import re
import sqlite3
import csv
import json
from lxml import etree

file_name = input('Input file name\n')


def clean_cell(cell_value):
    number = r'\d'
    corrected_value = ''
    for letter in cell_value:
        if re.match(number, letter):
            corrected_value += letter
    return int(corrected_value)


def clean(pd_object):
    counter = 0
    for i in pd_object.index:
        #print(f'i: {i}')
        for c in pd_object:
            #print(f'c: {c}')
            for n in str(pd_object.loc[i, c]):
                #print(f'valor: {pd_object.loc[i, c]}')
                if not re.match(r'\d', n):
                    counter += 1
                    pd_object.loc[i, c] = clean_cell(pd_object.loc[i, c])
                    break
    return counter


def convert_to_csv(in_file_name):
    sheet_xlsx = pd.read_excel(f'{in_file_name}', sheet_name='Vehicles', header=0, dtype=str)
    file_name_csv = in_file_name.replace(".xlsx", ".csv")
    sheet_xlsx.to_csv(f'{file_name_csv}', index=False, header=True)
    sheet_csv = pd.read_csv(f'{file_name_csv}', header=0)
    sheet_csv_shape = sheet_csv.shape
    print(f'{sheet_csv_shape[0]} lines were imported to {file_name_csv}' if sheet_csv_shape[
                                                                                0] > 1 else f'{sheet_csv_shape[0]} line was imported to {file_name_csv}')
    return pd.read_csv(f'{file_name_csv}', header=0)


def send_to_database(in_file_name):
    database_name = in_file_name.replace('[CHECKED].csv', '.s3db')
    conn = sqlite3.connect(database_name)
    cursor_name = conn.cursor()
    result = cursor_name.execute(r'CREATE TABLE IF NOT EXISTS convoy ('
                                 'vehicle_id INTEGER NOT NULL,'
                                 'engine_capacity INTEGER NOT NULL,'
                                 'fuel_consumption INTEGER NOT NULL,'
                                 'maximum_load INTEGER NOT NULL,'
                                 'score INTEGER NOT NULL,'
                                 'PRIMARY KEY(vehicle_id)'
                                 ')')
    with open(in_file_name, 'r') as fin:
        dr = list(csv.DictReader(fin))
        for row in dr:
            row['score'] = set_score(row)
            #print(row)


        to_db = [(i['vehicle_id'], i['engine_capacity'], i['fuel_consumption'], i['maximum_load'], i['score']) for i in
                 dr]
        #print(to_db)
        info = cursor_name.executemany(
            'INSERT OR REPLACE INTO convoy (vehicle_id, engine_capacity, fuel_consumption, maximum_load, score)'
            'VALUES (?, ?, ?, ?, ?);', to_db)
    conn.commit()
    print(
        f'{conn.total_changes} record was inserted into {database_name}' if conn.total_changes <= 1 else f'{conn.total_changes} records were inserted into {database_name}')
    conn.close()
    return database_name


def convert_to_json(database):
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    cursor_name = conn.cursor()
    rows = cursor_name.execute('''
    SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy
    WHERE score > 3;
    ''').fetchall()
    conn.commit()
    conn.close()
    file_json_name = database.replace('.s3db', '.json')
    with open(file_json_name, 'w') as json_file:
        json.dump({'convoy': [dict(ix) for ix in rows]}, json_file)
    print(f'{len(rows)} vehicles were saved into {file_json_name} file' if len(
        rows) > 1 else f'{len(rows)} vehicle was saved into {file_json_name} file')
    convert_to_xml(database)


def convert_to_xml(database):
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    cursor_name = conn.cursor()
    rows = cursor_name.execute('''
    SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy
    WHERE score <= 3;
    ''').fetchall()
    conn.commit()
    conn.close()
    file_xml_name = database.replace(".s3db", ".xml")
    if rows:
        dict_format = {'convoy': [dict(ix) for ix in rows]}
        #print(dict_format['convoy'])
        xml_text = ''
        for i in dict_format['convoy']:
            vehicle = ""
            for k, v in i.items():
                vehicle += f'<{k}>{v}</{k}>'
            xml_text += '<vehicle>' + vehicle + '</vehicle>'
        xml_text = '<convoy>' + xml_text + '</convoy>'
        #print(xml_text)
        root = etree.fromstring(xml_text)
        tree = etree.ElementTree(root)
        tree.write(file_xml_name)
        print(f'{len(dict_format["convoy"])} vehicles were saved into {file_xml_name} file' if len(dict_format["convoy"]) > 1 else f'{len(dict_format["convoy"])} vehicle was saved into {file_xml_name} file')
    else:
        xml_text = '<convoy> </convoy>'
        root = etree.fromstring(xml_text)
        tree = etree.ElementTree(root)
        tree.write(file_xml_name)
        print(f'0 vehicles were saved into {file_xml_name}')


'''
def convert_to_xml(json_file):
    with open(json_file) as json_format_file:
        data = json.load(json_format_file)
        xml_text = ''
        xml_file_name = json_file.replace('.json', '.xml')
        for i in data['convoy']:
            vehicle = ''
            for k, v in i.items():
                vehicle += f'<{k}>{v}</{k}>'
            xml_text += '<vehicle>' + vehicle + '</vehicle>'
        xml_text = '<convoy>' + xml_text + '</convoy>'
        # print(xml_text)
        root = etree.fromstring(xml_text)
        tree = etree.ElementTree(root)
        tree.write(xml_file_name)
        print(f'{len(data["convoy"])} vehicles were saved into {json_file.replace(".json", ".xml")} file' if len(data[
                                                                                                                     "convoy"]) > 1 else f'{len(data["convoy"])} vehicle was saved into {json_file.replace(".json", ".xml")} file')

'''
def set_score(row):
    total_fuel_needed = 450 * int(row['fuel_consumption']) / 100
    stops = total_fuel_needed / int(row['engine_capacity'])
    score = 0
    if stops < 1:
        score += 2
    if 1 <= stops < 2:
        score += 1
    if total_fuel_needed <= 230:
        score += 2
    if total_fuel_needed > 230:
        score += 1
    if int(row['maximum_load']) >= 20:
        score += 2
    return score


if file_name.endswith('.xlsx'):
    csv_file = convert_to_csv(file_name)
    counter = clean(csv_file)
    csv_file_name = f"{file_name.replace('.xlsx', '[CHECKED].csv')}"
    csv_file.to_csv(csv_file_name, header=True, index=False, encoding='utf-8')
    print(f'{counter} cells were corrected in {csv_file_name}' if counter > 1 else f'1 cell was corrected')
    convert_to_json(send_to_database(csv_file_name))

elif file_name.endswith('[CHECKED].csv'):
    convert_to_json(send_to_database(file_name))

elif file_name.endswith('.s3db'):
    convert_to_json(file_name)


else:
    csv_file = pd.read_csv(file_name, sep=',')
    counter = clean(csv_file)
    csv_file_name = f"{file_name.replace('.csv', '[CHECKED].csv')}"
    csv_file.to_csv(csv_file_name, header=True, index=False, encoding='utf-8')
    print(f'{counter} cells were corrected in {csv_file_name}' if counter > 1 else f'1 cell was corrected')
    convert_to_json(send_to_database(csv_file_name))
