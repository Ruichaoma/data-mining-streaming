import json
import time
from itertools import combinations
from pyspark import SparkConf, SparkContext
from operator import add
import random
import csv
import json
from collections import defaultdict
import os
import sys
from blackbox import BlackBox
import binascii
import numpy as np

input_filename = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_filename = sys.argv[4]


def judge_prime_number(potential):
    for i in range(2,int(potential**0.5)+1):
        if potential % i==0:
            return False
    return True

def get_prime_number(number):
    for i in range(number+1,number+20000):
        if judge_prime_number(i) == True:
            return i

num = int((69997/(stream_size*num_of_asks)) * np.log(2))
def create_hash(num):
    a = random.sample(range(1,69997),num)
    b = random.sample(range(1,69997),num)
    p = [get_prime_number(i) for i in random.sample(range(69997,169997),num)]
    hash_lst = []
    for i in range(num):
        hash_lst.append([a[i],b[i],p[i]])
    return hash_lst

def myhashs(s):
    complete_lst = []
    int_transfer = int(binascii.hexlify(s.encode('utf8')),16)
    hash_total = create_hash(num)
    filter_length = 69997
    for h in hash_total:
        complete_lst.append(((h[0]*int_transfer+h[1])%h[2])%filter_length)
    return complete_lst

time_count = 0
def bloom_filter(stream, ask):
    global userinfo
    global filterarray
    #global fp
    #global tn
    global time_count
    fp,tn = 0,0
    for i in stream:
        count_one = 0
        hashs = myhashs(i)
        for j in hashs:
            if filterarray[j] == 1:
                count_one += 1
            else:
                filterarray[j] = 1

        if i not in userinfo:
            if count_one != len(hashs):
                tn += 1
            else:
                fp += 1
        userinfo.add(i)
    if fp >0 or tn >0:
        false_positive_rate = float(fp)/(float(fp)+float(tn))
    else:
        false_positive_rate = 0.0

    with open(output_filename,'a') as fout:
        fout.write(str(time_count)+','+str(false_positive_rate)+'\n')

    time_count += 1

start_time = time.time()

userinfo = set()
filterarray = [0]*69997
with open(output_filename,'a') as fout:
    fout.write('Time,FPR'+'\n')

bx = BlackBox()
for i in range(num_of_asks):
    streaming_data = bx.ask(input_filename,stream_size)
    bloom_filter(streaming_data,i)

fout.close()
end_time = time.time()-start_time
print("Duration: "+str(end_time))











