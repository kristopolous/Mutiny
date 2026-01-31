#!/usr/bin/env python3
import discogs_client
import secrets
import redis
import json
import sys
import operator
import re
import pdb

def cachie(self):
  import requests_cache
  requests_cache.install_cache()

def expand(what):
  # I believe there's two properties that
  # suggest expansion.  There's the object
  # id and type and then there are resource_urls
  # that can lead to the expansions.
  #
  # perhaps some graph ql is the right 
  # approach here.
  #
  pass

discogs_client.fetchers.Fetcher.__init__ = cachie

labelMap = {}
_artistMap = {}

d = discogs_client.Client('ExampleApplication/0.1', user_token=secrets.USER_TOKEN)
r = redis.Redis(host='localhost', port=6379, db=0)

if len(sys.argv) < 2:
  print("I need a release")
  sys.exit(0)

search = sys.argv[1]
print(search)
res = d.search(search, type='release')

if len(res) == 0:
  search  = re.sub('^[0-9\. ]*', '', search)
  print(search)
  res = d.search(search, type='release')

def get_artists_for_label(label):
  artistMap = {}
  for release in label.releases:
    artist = release.data.get('artist')
    if artist not in artistMap:
      artistMap[artist] = 0
    artistMap[artist] += 1

  return artistMap

def artist_crawl(who):
  global _artistMap

for i in res:
  if type(i) is not discogs_client.models.Release:
    continue

  print("{}".format(i.title))
  for a in [*i.artists, *i.credits]:
    print("{:5}{}".format('', a.name))
    try:
      if a.name in _artistMap:
        _artistMap[a.name] += 1
        continue

      _artistMap[a.name] = 1
      relList = a.releases

    except:
      continue

    for rel in a.releases:
      #if type(rel) is discogs_client.models.Master:
      #  continue

      """
      for track in rel.tracklist:
        print(track.title, track.artists)
      """
      lname = rel.data.get('label')
      label = r.get('label:{}'.format(lname))
      print("{:3}{:40} - {:}".format('', rel.title[:40], lname))

      if label:
        urls = json.loads(label)
        if lname not in labelMap:
          labelMap[lname] = {'count':0, 'urls': urls}

        labelMap[lname]['count'] += 1

      else:
        for label in rel.labels:
          print("{:15}{}".format('', label.name))
          if label.name not in labelMap:
            labelMap[label.name] = {'count': 0, 'urls': label.urls}
          labelMap[label.name]['count'] += 1
          r.set('label:{}'.format(lname), json.dumps(label.urls))

          try:
            t_artistMap = get_artists_for_label(label)
            r.zadd('amap:{}'.format(lname), t_artistMap)
          except Exception as e:
            print("woops", e)
            continue



ttl = []
for name, label in labelMap.items():
  urlList = [re.sub('^.*?//([^/]*).*',r'\1',x) for x in label.get('urls') or []]
  for url in urlList:
    if 'bandcamp' in url:
      ttl.append([label.get('count'), url, name])

"""
ttl.sort(key=operator.itemgetter(0))

print("labels")
for row in ttl:
  print("{:3} {:80} {} ".format(*row))


print("\nartists")
for k,v in _artistMap.items():
  print("{:3} {}".format(v, k))

"""
