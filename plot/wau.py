# -*- coding: utf-8 -*-

import pygal
from pygal.style import DefaultStyle

start = False
start_line = 0
x = []
active = []
ios = []
android = []
us = []
nus = []
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('Weekly Active User Begin') != -1:
            start = True
        if line.find('Weekly Active User End') != -1:
            start = False
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            x.append(fields[1].strip().decode('utf-8'))
            active.append(int(fields[2].split(' ')[1]))
            ios.append(int(fields[3].split(' ')[1]))
            android.append(int(fields[4].split(' ')[1]))
            us.append(int(fields[5].split(' ')[1]))
            nus.append(int(fields[6].split(' ')[1]))

customStyle = DefaultStyle
customStyle.colors = ('#F44336', '#3F51B5', '#009688', '#FFC107', '#9C27B0',
                      '#9C27B0', '#03A9F4', '#8BC34A', '#FF9800', '#E91E63',
                      '#2196F3', '#4CAF50', '#FFEB3B', '#673AB7', '#00BCD4',
                      '#CDDC39', '#795548', '#9E9E9E', '#607D8B')
chart = pygal.Bar(human_readable=True, style=customStyle)
chart.title = u'周活数'
chart.x_labels = x
#chart.y_labels = [u'0', u'100万', u'200万', u'300万', u'400万', u'500万', u'600万', u'700万', u'800万', u'900万',
#                  u'1000万', u'1100万']
chart.add(u'周活总数', active)
chart.add(u'苹果', ios)
chart.add(u'安卓', android)
chart.add(u'美国', us)
chart.add(u'非美国', nus)
chart.render_to_file('/Users/tangrui/Documents/wau.svg')
