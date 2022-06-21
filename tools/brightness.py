#!/usr/bin/python3

from PIL import Image, ImageStat
import sys, math

def brightness( im_file ):
   im = Image.open(im_file)
   stat = ImageStat.Stat(im)
   r,g,b = stat.mean
   return math.sqrt(0.241*(r**2) + 0.691*(g**2) + 0.068*(b**2))

for line in sys.stdin:
  line = line.rstrip()
  try:
    print(brightness(line), line)
  except:
    pass
