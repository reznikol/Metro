#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from os.path import join, abspath
import pickle
import sqlite3
import os

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

#with open(jpath, 'rt', encoding='UTF8') as src:
    #schema = json.loads(src.read())
    
with open(jpath, 'rt', encoding='UTF8') as src:
    schema = json.load(src)
    
lines = {}
# m - метро
for m in schema['data']['lines']:
    lines[m['id']] = {
        'id': m['id'],
        'name': m['name']['ru'],
        'ordering': m['ordering'],
        'color': m['color'],
    }
    
stations = dict()
for m in schema['data']['stations']:
    stations[m['id']] = {
        'id': m['id'],
        'name': m['name']['ru'],
        'ordering': m['ordering'],
        'line_id': m['lineId'],
        'perspective': m['perspective'],
        'color': lines[m['lineId']]['color'],
    }
transitions = {}
for m in schema['data']['transitions']:
    transitions[m['id']]={
        'id': m['id'],
        'from_id': m['stationFromId'],
        'to_id': m['stationToId'],
        'perspective': m['perspective'],
        'bi': m['bi'],
        'length': m['pathLength']
    }

connections = {}
for m in schema['data']['connections']:
    connections[m['id']] = {
        'id':m['id'],
        'from_id': m['stationFromId'],
        'to_id': m['stationToId'],
        'length': m['pathLength'],
        'perspective': m['perspective'],
        'bi': m['bi'],
    }
    
gr = {}

for key in stations:
    gr[key] = {'name': station_name(key)}

for val in transitions.values():
    id1, id2, ln12 = val['from_id'], val['to_id'], val['length']
    pers, bi = val['perspective'], val['bi']
    if (id1 in gr) and (id2 in gr) and (not pers) and bi:
        gr[id1][id2] = ln12
        gr[id2][id1] = ln12

for val in connections.values():
    id1, id2, ln12 = val['from_id'], val['to_id'], val['length']
    pers, bi = val['perspective'], val['bi']
    if (id1 in gr) and (id2 in gr) and (not pers) and bi:
        gr[id1][id2] = ln12
        gr[id2][id1] = ln12    

#проверка на наличие станций без названий

#print(station_name(44))
#print(station_name(176))
#print(station_name(313))
#print(station_name(515))

#for n in range(550):
    #print(f' n = {n} --> {station_name(n)}')
    
print('Все данные по станциям получены.')

data_path = abspath(join('..', 'Data', 'scheme.pickle'))
print(data_path)
with open(data_path, 'wb') as dst:
    pickle.dump(gr, dst)
    pickle.dump(lines, dst)
    pickle.dump(stations, dst)
print(f'Все данные сохранены в {data_path} файл.')

base_path = abspath(join('..', 'Data', 'scheme.sqlite3'))
print(base_path)
try:
    os.remove(base_path)
except FileNotFoundError:
    pass

con = sqlite3.connect(base_path)
cur = con.cursor()

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
try:
    cur.executescript(sql)
except sqlite3.DatabaseError as err:
    print(f'Ошибка №1: {err}')
else:
    print(f'База данных {base_path} успешно создана')

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
    con.commit()
    print(f'Все запросы добавления данных успешно выполнены')
con.close()

print('END')