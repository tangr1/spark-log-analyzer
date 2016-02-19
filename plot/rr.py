# -*- coding: utf-8 -*-

import pygal

start = False
start_line = 0
x = []
rr = [[], [None], [None, None], [None, None, None], [None, None, None, None], [None, None, None, None, None],
      [None, None, None, None, None, None]]
ios = []
android = []
us = []
nus = []
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('Retention Rate Begin') != -1:
            start = True
        if line.find('Retention Rate End') != -1:
            start = False
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            #x.append(fields[1].strip().decode('utf-8'))
            for i in range(3, len(fields)):
                a = fields[i].strip().strip("*")
                if len(a) > 0:
                    rr[start_line - 4].append(float(a.split('%')[0]))

chart = pygal.Line(x_label_rotation=15)
chart.title = u'日留存率'
chart.x_labels = [u'2.4 周四', u'2.5 周五', u'2.6 周六', u'2.7 周日', u'2.8 周一', u'2.9 周二', u'2.10 周三', u'2.11 周四',
                  u'2.12 周五', u'2.13 周六', u'2.14 周日', u'2.15 周一', u'2.16 周二', u'2.17 周三']
chart.y_labels = ['10%', '20%', '30%', '40%', '50%']
chart.add(u'2.3 周三', rr[0])
chart.add(u'2.4 周四', rr[1])
chart.add(u'2.5 周五', rr[2])
chart.add(u'2.6 周六', rr[3])
chart.add(u'2.7 周日', rr[4])
chart.add(u'2.8 周一', rr[5])
chart.add(u'2.9 周二', rr[6])
chart.render_to_file('/Users/tangrui/Documents/rr.svg')
#for i in range(1, 8):
#    chart = pygal.Line(x_label_rotation=15)
#    chart.title = u'2月' + unicode(str(i+2)) + u'号日留存率'
#    chart.x_labels = [u'1日 周四', u'2日 周五', u'3日 周六', u'4日 周日', u'5日 周一', u'6日 周二', u'7日 周三', u'8日 周四',
#                       u'9日 周五', u'10日 周六', u'11日 周日', u'12日 周一', u'13日 周二', u'14日 周三'][i - 1:]
#    chart.add(u'日留存率', rr[i - 1])
#    chart.render_to_file('/Users/tangrui/Documents/rr' + str(i) + '.svg')
