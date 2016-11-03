from multiprocessing import Process
import numpy as np
import random


dataset = 10000  # how many unique ids / rows for dataset
threads = 4  # how many threads to run, should equal cpu cores


interval = dataset / threads
start = 0
stop = interval


def write_file(start, stop, i):
	my_line = ""
	print("Generating Data in memory for part {0}".format(i))
	count = stop - start

	# create income array
	income_avg, income_range  = 100000,20000
	income = np.random.normal(income_avg,income_range,count)

	# create age array
	age_avg, age_range = 40,20
	age = np.random.normal(age_avg,age_range,count)

	# create credit_core array
	credit_avg, credit_range = 740, 40
	credit = np.random.normal(credit_avg, credit_range, count)
	for y in range(count):
		my_line += "{0},{1},{2},{3}\n".format(str(start + y), str(format(income[y], '.2f')), str(int(age[y])), str(int(credit[y])))

	print("Writing from {0} to {1} in loan_applications_{2}.csv\n".format(start, stop, i))
	with open("loan_applications_{0}.csv".format(i), 'w') as f:
		f.write(my_line)
	print("Finished writing to loan_applications_{0}.csv\n".format(i))


for i in range(threads):
	p = Process(target=write_file, args=(start, stop, i))
	p.start()
	start += interval
	stop += interval

p.join()
