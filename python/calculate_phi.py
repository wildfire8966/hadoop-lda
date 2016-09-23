#!/usr/bin/env python
import sys, math
import time
import datetime


if __name__=='__main__':
	for line in sys.stdin:
		v = line.strip().split(' ')
		if len(v) < 2:
			continue
		sum = 0
		topic_count = {}
		for i in range(1, len(v)):
			#print int(v[i])
			if v[i] == '0':
				continue
			sum += int(v[i])
			topic_count[i - 1] = int(v[i])
		
		log_sum = math.log10(sum + 1)
		sys.stdout.write(v[0])
		for t, c in topic_count.items():
			#print t,c
			prob = math.log10(c) - log_sum
			sys.stdout.write('\t' + str(t) + ':' + str(prob))
		sys.stdout.write('\n')
	rm = '''
	#for line in sys.stdin:
	for line in open(sys.argv[1]):
		v = line.strip().split('\t')
		if len(v) < 2:
			continue
		print v[0] + '\t' + str(float(v[1]) / total)
	'''
