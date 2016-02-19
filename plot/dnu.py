# -*- coding: utf-8 -*-

import pygal

start = False
start_line = 0
x = []
dnu = []
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('Event Analysis Begin') != -1:
            start = True
        if line.find('Event Analysis End') != -1:
            start = False
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            x.append(fields[1].strip().decode('utf-8'))
            dnu.append(int(fields[10].strip()))

chart = pygal.Line(x_label_rotation=30, human_readable=True)
chart.title = u'新增用户'
chart.x_labels = x
#chart.y_labels = [u'8万', u'12万', u'16万', u'20万', u'24万', u'28万', u'32万', u'36万', u'40万']
chart.add(u'新增用户', dnu)
chart.render_to_file('/Users/tangrui/Documents/dnu.svg')
