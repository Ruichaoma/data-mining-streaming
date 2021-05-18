from blackbox import BlackBox
import random
import sys
import time


start_time = time.time()

#input_filename = "/Users/ruichao/Desktop/hw1/users.txt"
#stream_size = 100
#num_of_asks = 30
#output_filename = "/Users/ruichao/Desktop/task3nb.csv"

input_filename = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_filename = sys.argv[4]

random.seed(553)
first_100 = []
seqnum = 0
count = 0
max_size = 100
def reservoir(stream):
    global first_100
    global seqnum
    global count
    for user_id in stream:
        seqnum += 1
        if len(first_100)<max_size:
            first_100.append(user_id)
        else:
            if random.random() < stream_size/seqnum:
                replace_index = random.randint(0, 99)
                first_100[replace_index] = user_id

    fout.write(str(seqnum) + ',' + str(first_100[0]) + ',' + str(first_100[20]) + ',' + str(first_100[40]) + ',' + str(
        first_100[60]) + ',' + str(first_100[80]) + '\n')

    count += 1



fout = open(output_filename, "w")
fout.write("seqnum,0_id,20_id,40_id,60_id,80_id"+'\n')



bx = BlackBox()
for i in range(num_of_asks):
    stream_users = bx.ask(input_filename, stream_size)
    reservoir(stream_users)
fout.close()
print("Duration : ", time.time() - start_time)













