#!/usr/bin/python
import sys

for line in sys.stdin:
	line = line.strip()
	for word in line.split():
		val = "{0},{1}".format(word,1)
		print val
