#! /usr/bin/python
import sys

for line in sys.stdin :
	lineSplited=line.split(" ")
	for word in lineSplited:
		wordLength=len(word)
		print "%s\t%s" % (word[0],wordLength)

