# -*- coding: utf-8 -*-

import pygal

start = False
start_line = 0
titles = []
x = []
dea = [[], [], [], [], [], [], [], [], [], [], []]
with open('/Users/tangrui/Documents/musically.md', 'r') as f:
    for line in f:
        if start:
            start_line += 1
        if line.find('Event Analysis Begin') != -1:
            start = True
        if line.find('Event Analysis End') != -1:
            start = False
        if start and start_line == 2:
            fields = line.split('|')
            for i in range(2, len(fields)):
                titles.append(fields[i].strip())
        if start and start_line > 3 and len(line.strip()) > 0:
            fields = line.split('|')
            x.append(fields[1].strip().decode('utf-8'))
            for i in range(2, len(fields)):
                a = fields[i].strip()
                if len(a) > 0:
                    dea[i - 2].append(long(a))

for i in range(0, 11):
    chart = pygal.Line(x_label_rotation=30, human_readable=True)
    chart.title = titles[i]
    chart.x_labels = x
    chart.add(u'事件数', dea[i])
    chart.render_to_file('/Users/tangrui/Documents/dea' + str(i) + '.svg')

chart = pygal.Line(x_label_rotation=30, human_readable=True)
chart.title = u'评论事件'
chart.x_labels = x
chart.add(u'创建', dea[0])
chart.add(u'删除', dea[1])
chart.add(u'点赞', dea[2])
chart.add(u'取消点赞', dea[3])
chart.render_to_file('/Users/tangrui/Documents/comment.svg')

chart = pygal.Line(x_label_rotation=30, human_readable=True)
chart.title = u'Musical创建与删除事件'
chart.x_labels = x
chart.add(u'创建', dea[6])
chart.add(u'删除', dea[4])
chart.render_to_file('/Users/tangrui/Documents/musical1.svg')

chart = pygal.Line(x_label_rotation=30, human_readable=True)
chart.title = u'Musical点赞与取消点赞事件'
chart.x_labels = x
chart.add(u'点赞', dea[5])
chart.add(u'取消点赞', dea[7])
chart.render_to_file('/Users/tangrui/Documents/musical2.svg')

chart = pygal.Line(x_label_rotation=30, human_readable=True)
chart.title = u'用户事件'
chart.x_labels = x
#chart.add(u'创建', dea[8])
chart.add(u'关注', dea[9])
chart.add(u'取消关注', dea[10])
chart.render_to_file('/Users/tangrui/Documents/user.svg')
