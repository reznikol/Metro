#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# Программа для чтения и анализа файла json, построения графа
# и сохранения информации на диск с помощью модулей pickle и sqlite

import json
from os.path import join, abspath
import pickle
import sqlite3
import os


# Функция для совместного отображения названия станции и названия линии
def station_name(id_st):
    try:
        st_text = stations[id_st]['name']
    except KeyError:
        st_text = f'<<< { id_st } >>>'
    try:
        lin_text = lines[stations[id_st]['line_id']]['name']
    except KeyError:
        lin_text = '?????????'
    
    result = st_text + '(' + lin_text + ')'
    return result

jpath = abspath(join('..', 'Data', 'scheme.json'))
print(jpath)

# Открываем файл json на чтение и весь его перегружаем в переменную schema
with open(jpath, 'rt', encoding='UTF8') as src:
    schema = json.load(src)

lines = {} # Создаем пустой словарь для линий и наполняем его информацией о линиях из переменной schema
for m in schema['data']['lines']:
    lines[m['id']] = {
        'id': m['id'],
        'name': m['name']['ru'],
        'ordering': m['ordering'],
        'color': m['color'],
    }

stations = {} # Создаем пустой словарь для станций и наполняем его информацией о станциях из переменной schema
for m in schema['data']['stations']:
    stations[m['id']] = {
        'id': m['id'],
        'name': m['name']['ru'],
        'ordering': m['ordering'],
        'line_id': m['lineId'],
        'perspective': m['perspective'],
        'color': lines[m['lineId']]['color'],
    }

transitions = {} # Создаем пустой словарь для станций и наполняем его информацией о переходах из переменной schema
for m in schema['data']['transitions']:
    transitions[m['id']]={
        'id': m['id'],
        'from_id': m['stationFromId'],
        'to_id': m['stationToId'],
        'perspective': m['perspective'],
        'bi': m['bi'],
        'length': m['pathLength']
    }

connections = {} # Создаем пустой словарь для станций и наполняем его информацией о пересадках из переменной schema
for m in schema['data']['connections']:
    connections[m['id']] = {
        'id':m['id'],
        'from_id': m['stationFromId'],
        'to_id': m['stationToId'],
        'length': m['pathLength'],
        'perspective': m['perspective'],
        'bi': m['bi'],
    }

gr = {} # Создаем граф

for key in stations: # Вносим данные о станциях
    gr[key] = {'name': station_name(key)}

for val in transitions.values(): # Вносим данные о переходах
    id1, id2, ln12 = val['from_id'], val['to_id'], val['length']
    pers, bi = val['perspective'], val['bi']
    if (id1 in gr) and (id2 in gr) and (not pers) and bi:
        gr[id1][id2] = ln12
        gr[id2][id1] = ln12

for val in connections.values(): # Вносим данные о пересадках
    id1, id2, ln12 = val['from_id'], val['to_id'], val['length']
    pers, bi = val['perspective'], val['bi'] # Если обе станции (по номерам id) существуют, построены и не являются перспективными
    if (id1 in gr) and (id2 in gr) and (not pers) and bi:
        gr[id1][id2] = ln12
        gr[id2][id1] = ln12    

print('Все данные по станциям получены.')

data_path = abspath(join('..', 'Data', 'scheme.pickle')) # Задаем путь для сохранения файла pickle
print(data_path)

with open(data_path, 'wb') as dst: # Открываем файл pickle обязательно в бинарном виде
    pickle.dump(gr, dst)
    pickle.dump(lines, dst)
    pickle.dump(stations, dst)
print(f'Все данные сохранены в {data_path} файл.')

base_path = abspath(join('..', 'Data', 'scheme.sqlite3')) # Задаем путь для сохранения файла sqlite3
print(base_path)

# Удаляем файл базы данных, если он существует
try: 
    os.remove(base_path)
except FileNotFoundError:
    pass

con = sqlite3.connect(base_path) # Подключаемся к базе данных (создаем пустой файл базы данных)
cur = con.cursor()

# Формируем запрос для создания структуры базы данных
sql = '''
CREATE TABLE gr(
    stn_id INTEGER PRIMARY KEY,
    name TEXT
);
CREATE TABLE stn12_time(
    stn12_id INTEGER PRIMARY KEY,
    stn1 INTEGER,
    stn2 INTEGER,
    timen INTEGER
);
CREATE TABLE lines(
    line_id INTEGER PRIMARY KEY,
    color TEXT,
    id INTEGER,
    name TEXT,
    ordering INTEGER
);
CREATE TABLE stations(
    stations_id INTEGER PRIMARY KEY,
    color TEXT,
    id INTEGER,
    line_id INTEGER,
    name TEXT,
    ordering INTEGER,
    perspective INTEGER
);
'''

# Формируем структуру базы данных на основании запроса
try:
    cur.executescript(sql)
except sqlite3.DatabaseError as err:
    print(f'Ошибка №1: {err}')
else:
    print(f'База данных {base_path} успешно создана')

# Подготавливаем данные для помещения в базу данных
try:
    # Таблица gr
    for x in gr:
        sdata = x, gr[x]['name']
        sql = 'INSERT INTO gr (stn_id, name) VALUES (?, ?)'
        cur.execute(sql, sdata)
        
    # Таблица stn12_time
    for x1 in gr:
        for x2 in gr[x1]:
            try:
                _ = int(x2)
                sdata = x1, x2, gr[x1][x2]
                sql = 'INSERT INTO stn12_time (stn1, stn2, timen) VALUES (?, ?, ?)'
                cur.execute(sql, sdata)
            except ValueError:
                continue
            
    # Таблица lines
    for x in lines:
        sdata = (
            x, 
            lines[x]['color'],
            lines[x]['id'],
            lines[x]['name'],
            lines[x]['ordering'],
        )
        sql = 'INSERT INTO lines (line_id, color, id, name, ordering) VALUES (?, ?, ?, ?, ?)'
        cur.execute(sql, sdata)
        
    # Таблица stations
    for x in stations:
        sdata = (
            x, 
            stations[x]['color'],
            stations[x]['id'],
            stations[x]['line_id'],
            stations[x]['name'],
            stations[x]['ordering'],
            stations[x]['perspective'],
        )
        sql = 'INSERT INTO stations (stations_id, color, id, line_id, name, ordering, perspective) VALUES (?, ?, ?, ?, ?, ?, ?)'
        cur.execute(sql, sdata)
except sqlite3.DatabaseError as err:
    print(f'Ошибка №2: {err}')
else:
    con.commit()  # Непосредственно запись в базу
    print(f'Все запросы добавления данных успешно выполнены')
con.close()
