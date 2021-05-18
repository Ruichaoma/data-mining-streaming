from blackbox import BlackBox
import sys
import time
import binascii
import random
from statistics import mean, median
from itertools import combinations
from pyspark import SparkConf, SparkContext
from operator import add
import binascii


input_file = sys.argv[1]
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

        
def create_hash(num):
    a = random.sample(range(1,2**16),num)
    b = random.sample(range(1,2**16),num)
    p = [get_prime_number(i) for i in random.sample(range(2**16,2**16+100000),num)]
    hash_lst = []
    for i in range(num):
        hash_lst.append([a[i],b[i],p[i]])
    return hash_lst
def abp(n):
    a = random.sample(range(1, 2500), n)
    b = random.sample(range(1, 2500), n)
    p = [get_prime_number(i) for i in random.sample(range(2**16,2**16+100000),n)]
    hash_lst = []
    for i in range(n):
        hash_lst.append([a[i], b[i], p[i]])
    return hash_lst


def myhashs(s):
    complete_lst = []
    transfer_int = int(binascii.hexlify(s.encode('utf8')), 16)
    num = 16
    hash_lst = abp(num)
    m = 2 ** num
    for h in hash_lst:
        complete_lst.append(((h[0] * transfer_int + h[1]) % h[2]) % m)
    return complete_lst


def median_avg(number):
    avg_lst = []
    initial = 0
    for endpoint in range(4, num):
        part = number[initial:endpoint]
        mean_part = mean(part)
        avg_lst.append(mean_part)
        initial = endpoint
        endpoint += 4
    median_value = median(avg_lst)
    return median_value


def zero_count(hashs):
    transfer_to_bin = bin(hashs)
    valid_bin = transfer_to_bin[2:]
    valid_bin_nonzero = valid_bin.rstrip("0")
    zero_count = len(valid_bin)-len(valid_bin_nonzero)
    return zero_count
    

count = 0
def flajolet(stream):
    global truth
    global estimated
    global count
    estimations = []
    hash_lst = []
    for i in stream:
        hash_values = myhashs(i)
        hash_lst.append(hash_values)
    for i in range(num):
        max_zero = -float('inf')
        for j in hash_lst:
            zero_counting = zero_count(j[i])
            if zero_counting > max_zero:
                max_zero = zero_counting
        estimations.append(2 ** max_zero)
    initial = 0
    avg_lst = []
    for k in range(4,num):
        group = estimations[initial:k]
        mean_group = mean(group)
        avg_lst.append(mean_group)
        initial = k
        k += 4
    total_estimateeee = median(avg_lst)
    total_estimate = int(median_avg(estimations))
    estimated += total_estimate
    len_stream = len(set(stream))
    truth += len(set(stream))
    fout.write(str(count) + "," + str(len_stream) + "," + str(total_estimate) + "\n")
    count+=1



start_time = time.time()



truth,estimated = 0,0



num = 2**4

fout = open(output_filename, "w")
fout.write("Time,Ground Truth,Estimation"+"\n")

#with open(output_file,'a') as f:
 #   f.write("Time,Ground Truth,Estimation"+"\n")

bx = BlackBox()
for i in range(num_of_asks):
    streaming_data = bx.ask(input_file, stream_size)
    flajolet(streaming_data)
fout.close()

print("Duration : ", time.time() - start_time)


























