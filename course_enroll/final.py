from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('my_app').setMaster('local')
sc = SparkContext(conf = conf)

import sys

def CourseEnrollmentEventsPerDayMixin(key, values):
	rdd1 = sc.parallelize([x for x in values])
	rdd2 = rdd1.sortByKey(ascending = False)
	rdd3 = sc.parallelize(key)

	'''
	yields will be :
	(rdd3.collect()[0], rdd2.collect()[0].map(lambda x : x.split('T'))[0]), rdd2.collect()[1][1]	
	i.e. (course_id, datestamp), enrollment_change

	'''

	f = open('temporary1.txt','a')
	f.write('(' + '(' + '\"' + rdd3.collect()[0] + '\"' + ',' + '\"' + rdd2.collect()[0][0].split('T')[0] + '\"' + '),' + str(rdd2.collect()[0][1])+ ')' + '\n')
	f.close()
	
def CourseEnrollmentChangesPerDayMixin():

	tmp_rdd = sc.textFile('temporary1.txt')
	tmp_rdd1 = tmp_rdd.map(lambda z : eval(z))
	rdd5 = tmp_rdd1.reduceByKey(lambda x, y : int(x) + int(y))
	
	'''
	yiels will be:
	(rdd5.collect()[0][0], rdd5.collect()[0][1]), rdd5.collect()[1][0]
	i.e. (course_id, datestamp), enrollment_change

	'''
	
	f2 = open(sys.argv[2],'a')
	f2.write('(' + str(rdd5.collect()[0][0][0]) + ',' + str(rdd5.collect()[0][0][1]) + ')' + ',' + str(rdd5.collect()[0][1]) + '\n')
	f2.close()

if __name__ == '__main__':
	inp = sc.textFile(sys.argv[1])
	inp1 = inp.map(lambda x : eval(x))
	tmp = inp1.groupByKey().collect()
	for i in range(len(tmp)):
		CourseEnrollmentEventsPerDayMixin(tmp[i][0], tmp[i][1])  #passes key:  (course_id, user_id) tuple and
									 #values:  iterator of (timestamp, action_value) tuples	
	CourseEnrollmentChangesPerDayMixin()
