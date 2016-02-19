# -*- coding: utf-8 -*-

import pygal

start = False
start_line = 0
x = []
mau = [[]]
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('Monthly Active User Begin') != -1:
            start = True
        if line.find('Monthly Active User End') != -1:
            start = False
        if start and start_line == 2:
            fields = line.split('|')
            for i in range(7, len(fields) - 1):
                x.append(fields[i].strip().decode('utf-8'))
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            for i in range(7, len(fields) - 1):
                mau[0].append(float(fields[i].split('%')[0].split('(')[1].strip()))

chart = pygal.Pie()
chart.title = u'1.20-2.16月活组成'
for i in range(0, len(x)):
    chart.add(x[i], mau[0][i])
chart.render_to_file('/Users/tangrui/Documents/mau.svg')
