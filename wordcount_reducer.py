#!/usr/bin/python

import sys

wordcount = {}
for line in sys.stdin:
	line = line.strip()
	word,count = line.split(',')
	if word in wordcount :
		wordcount[word] += int(count)
	else:
		wordcount[word] = int(count)
for key in wordcount:
	print "{0},{1}".format(key,wordcount[key])
