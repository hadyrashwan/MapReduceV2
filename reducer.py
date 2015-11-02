#! /usr/bin/python
import sys
temp=""
characterSum = 0 # sums will be stored here 
numberOfValues= 0  # just a counter 
for line in sys.stdin:
	key , value=line.split("\t") 
	if temp=="" :
		temp=key 
	if temp!=key :
			characterAvg = float(characterSum)/numberOfValues # get the sum
			print "%s\t%s" %(temp, characterAvg)  # pass the output to  the outputFormate 
			numberOfValues=0
			characterSum=0.0
			characterAvg=0.0
			temp = key
	numberOfValues+=1
	characterSum=characterSum+float(value)
characterAvg = float(characterSum)/numberOfValues # get the sum
print "%s\t%s" %(temp, characterAvg)  # pass the output to  the outputFormate 
			