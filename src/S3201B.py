#!/usr/bin/env python3
# -*- coding: utf-8 -*-8

from os.path import join, abspath
import pickle
from prettytable import PrettyTable
from S3201A import station_name

def spros(nach_kon):
    #*****************spros******************
    temp_lines = [(x['ordering'], x['id'], x['name']) for x in lines.values()]
    temp_lines.sort()
    
    templn = {}
    
    myTable = PrettyTable(['№ линии', 'Название линии'])
    for (no, nl, lin_name), nr in zip(temp_lines, range(1, 100)): #сделали с помощью map порядклвые номера станций так как их id идут не по порядку
        templn[nr] = nl
        myTable.add_row([nr, lin_name])
        print(myTable)
        
    nline = int(input(f'\nВведите номер линии {nach_kon}ой станции: '))
    nline = templn[nline]
    print(f'Выбрана {nach_kon}ая линия: {lines[nline]["name"]}\n')
    
    tempst  = {}
    n = 0
    myTable = PrettyTable(['№ станции','Название станции','Id_st'])
    for sta in stations.values():
        if sta['line_id'] == nline:
            n += 1
            myTable.add_row([n, station_name(sta['id']), sta['id']])
            tempst[n] = sta['id']
    myTable.align['Название станции'] = 'l'
    print(myTable)
    nst = int(input(f'\nВведите номер {nach_kon}ой станции: '))
    nst1 = tempst[nst]

    return nst1

def findminp(nst1, nst2):
    u, s, p = set(), {nst1: 0}, {}
    while True:
        n, m = None, None
        for x in s:
            if x not in u:
                if n == None:
                    n, m = x, s[x]
                else:
                    if s[x] < m:
                        n, m = x, s[x]
        if n == None:
            return None
        if n == nst2:
            result = [nst2]
            x = nst2
            while x != nst1:
                x = p[x]
                result.append(x)
            return result, s[nst2]
        u.add(n)
        for x in gr[n]:
            try:
                _ = int(x)
            except ValueError:
                continue
            if x not in u:
                if x not in s:
                    s [x], p[x] = gr[n][x] + m, n
                else:
                    if s[x] > gr[n][x] +m:
                        s [x], p[x] = gr[n][x] + m, n
    

data_path = abspath(join('..', 'Data', 'scheme.pickle'))

with open(data_path, mode='rb') as dst:
    gr = pickle.load(dst)
    lines = pickle.load(dst)
    stations = pickle.load(dst)
print(f'Все данные из {data_path} по станциям и линиям восстановлены.')

# Организация диалога с пользователем
nst1 = spros('начальн')

nst2 = spros('конечн')

print(f'\nВыбрана начальная станция {station_name(nst1)} {nst1}')
print(f'Выбрана конечная станция {station_name(nst2)} {nst2}')

print(f'Ищем минимальный путь между {nst1} и {nst2}')
nst1, nst2 = nst2, nst1
res = findminp(nst1, nst2)

if res:
    myTable = PrettyTable(['Время', 'Название станции', 'Id_st'])
    mpas, minp = res
    xold = None
    tx = 0
    for x in mpas:
        if xold:
            tx += gr[xold][x]
        else:
            tx = 0
        myTable.add_row([
            f'{tx//3600:02d}:{(tx%3600)//60:02d}:{tx%60:02d}',
            f'Станция {station_name(x)}',
            f'({x})'
        ])
        xold = x
    myTable.align['Название станции'] = 'r'
    print(myTable)
    print(
        f'\nОбщее время пути: {minp//3600:02d} час. '
        f'{(minp%3600)//60:02d} мин. и {minp%60:02d} сек.'
        )
else:
    print('Путь между станциями не найден')

print('END')