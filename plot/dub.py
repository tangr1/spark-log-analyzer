# -*- coding: utf-8 -*-

import pygal

start = False
start_line = 0
x = []
dub = [[]]
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('User Behavior Begin') != -1:
            start = True
        if line.find('User Behavior End') != -1:
            start = False
        if start and start_line == 2:
            fields = line.split('|')
            for i in range(3, len(fields) - 1):
                x.append(fields[i].strip().decode('utf-8'))
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            for i in range(3, len(fields) - 1):
                dub[0].append(float(fields[i].split('%')[0].strip()))

chart = pygal.Pie()
chart.title = u'2.3用户行为分析'
for i in range(0, len(x)):
    chart.add(x[i], dub[0][i])
chart.render_to_file('/Users/tangrui/Documents/dub.svg')
