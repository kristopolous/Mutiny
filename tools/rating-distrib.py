#!/usr/bin/env python3
import sys, os
from functools import reduce 

thresh = int(sys.argv[1]) if len(sys.argv) > 1 else 5
width = 70
longestLabel = 0
labelMap = {}
labelTotal = {}
fracList = []
rateList = ['__rating_5', '__rating_4', '__rating_3', '__purge']

with open('.listen_all', 'r') as f:
  for line in f.readlines():
    parts = line.split(' ')
    label = parts[0].split('/')[0]

    labelTotal[label] = (labelTotal.get(label) or 0) + 1


with open('.listen_done', 'r') as f:
  for line in f.readlines():
    parts = line.split(' ')
    label = parts[0].split('/')[0]
    rating = parts[1].strip()

    if rating == '__skipping':
      rating = '__rating_3'

    if rating not in rateList:
      continue

    if not label in labelMap:
      labelMap[label] = {}

    labelMap[label][rating] = (labelMap[label].get(rating) or 0) + 1

for label in labelMap:
  total = reduce(lambda x, y: x + y, labelMap[label].values(), 0)
  if total > thresh:
    longestLabel = max(longestLabel, len(label))
    row = {'weight': 0, 'label': label, 'total': total}
    for k,v in labelMap[label].items():
      row[k] = v / total
    
    num = len(rateList)
    for i in rateList:
      row['weight'] += num * (row.get(i) or 0)
      num -= 1

    fracList.append(row)

labelStr = "{:%d}" % (longestLabel)
fracSort = sorted(fracList, key = lambda i: i.get('weight') or 0)
cnt = 0
for row in fracSort:
  graph = ''
  accum = 0
  start = 9608

  for i in rateList:
    accum += row.get(i) or 0
    inst = round(accum * width) - len(graph)
    draw = chr(183) if i == '__purge' else chr(start)
    graph += draw * inst
    start += 3

  perc = percFloat = 0
  if labelTotal.get(row['label']):
    percFloat = row['total'] / labelTotal.get(row['label'])
    perc = min(1, int(10 * percFloat))


  """
  if os.path.exists(row['label']):
    if os.path.isfile("{}/no".format(row['label'])):
      continue
    # sz = os.popen("du -sm {}".format(row['label'])).read().split('\t')[0]
  else:
    continue
  """

  cnt += 1
  rev = '' if cnt % 3 else chr(27) + '[4m'
  reset = chr(27) + '[0m'

  print(rev + " " + labelStr.format(row['label']), 
    reset + " " + graph, 
    rev, 
    "{:3} {}{} {:>4.0f}% {:>5}".format(
      row['total'], 
      chr(9642) * perc, 
      chr(903) * (10-perc), 
      100*percFloat, 0 #, sz
    ), reset)
