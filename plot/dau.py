# -*- coding: utf-8 -*-

import pygal

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
        if line.find('Daily Active User Begin') != -1:
            start = True
        if line.find('Daily Active User End') != -1:
            start = False
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            x.append(fields[1].strip().decode('utf-8'))
            active.append(int(fields[2].split(' ')[1]))
            ios.append(int(fields[3].split(' ')[1]))
            android.append(int(fields[4].split(' ')[1]))
            us.append(int(fields[5].split(' ')[1]))
            nus.append(int(fields[6].split(' ')[1]))

chart1 = pygal.Line(x_label_rotation=30, human_readable=True)
chart1.title = u'日活数（苹果，安卓）'
chart1.x_labels = x
#chart1.y_labels = [u'200万', u'300万', u'400万', u'500万', u'600万', u'700万']
chart1.add(u'日活总数', active)
chart1.add(u'苹果', ios)
chart1.add(u'安卓', android)
chart1.render_to_file('/Users/tangrui/Documents/dau1.svg')

chart2 = pygal.Line(x_label_rotation=30, human_readable=True)
chart2.title = u'日活数（区域a，区域b）'
chart2.x_labels = x
#chart2.y_labels = [u'200万', u'300万', u'400万', u'500万', u'600万', u'700万']
chart2.add(u'日活总数', active)
chart2.add(u'区域a', us)
chart2.add(u'区域b', nus)
chart2.render_to_file('/Users/tangrui/Documents/dau2.svg')
