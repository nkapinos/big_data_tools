from multiprocessing import Process
import random


dataset = 1000000  # how many unique ids / rows for dataset
threads = 8  # how many threads to run, should equal cpu cores


interval = dataset / threads
start = 0
stop = interval


def write_file(start, stop, i):
	with open("file_{0}.csv".format(i), 'w') as f:
		print("Writing from {0} to {1} in file_{2}.csv\n".format(start, stop, i))
		x = start
		while( x < (stop + 1)):
			age = random.randint(18,90)
			income = random.randint(40000,100000)
			carloan = random.randint(5000,60000)
			retirement = random.randint(50000,2000000)
			studloan = random.randint(10000,200000)
			income2 = random.randint(40000,100000)
			carloan2 = random.randint(5000,60000)
			retirement2 = random.randint(50000,2000000)
			studloan2 = random.randint(10000,200000)
			f.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\n".format(x, age, income, carloan, retirement, studloan, income2, carloan2, retirement2, studloan2))
			x += 1
		print("Finished writing to file_{0}.csv\n".format(i))


for i in range(threads):
	p = Process(target=write_file, args=(start, stop, i))
	p.start()
	start += interval
	stop += interval

p.join()
