from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('my_app1').setMaster('local')
sc = SparkContext(conf = conf)

import sys

def ProblemCheckEventMixin(key, values):
	rdd1 = sc.parallelize([x for x in values])
        rdd2 = rdd1.sortByKey()
        rdd3 = sc.parallelize(key)

	for answer in _generate_answers(rdd2.collect()[0][1], 'first'):
		
		f1 = open(sys.argv[2],'a')
#		f1.write('For first attempt :------------------------' + '\n')
		f1.write(str(answer) + '\n')				#output key : (course_id, answer_id)
		f1.close()                                              #output values : (timestamp, answer_data)
		'''
		print("{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{")
		print answer
		print("}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}")
	
		'''
	
	for answer in _generate_answers(rdd2.collect()[-1][1], 'last'):
		f2 = open(sys.argv[2],'a')
#		f2.write('For last attempt :--------------------------' + '\n')
		f2.write(str(answer) + '\n')
		f2.close()

def _generate_answers(event_string, attempt_category):
	rdd_event_string = sc.parallelize([event_string])

#	print("***********************************************************************************")
#	print rdd_event_string.collect()

#	rdd_event = sc.parallelize(json.loads(str(rdd_event_string.collect()[0])).items())
	rdd_event = sc.parallelize(rdd_event_string.collect()[0].items())
	
	course_id = sc.parallelize(rdd_event.lookup('context')[0].items()).lookup('course_id')[0]
	timestamp = rdd_event.lookup('time')[0]       #in logs file it is 'time' not 'timestamp'
	problem_id = rdd_event.lookup('problem_id')[0]     #problem_id key is under the event key of the log file 218 
	problem_display_name = sc.parallelize(sc.parallelize(rdd_event.lookup('context')[0].items()).lookup('module')[0].items()).lookup('display_name')[0]
	result = []
	rdd_answers = sc.parallelize(rdd_event.lookup('answers')[0].items())


	def append_submission(answer_id, rdd_submission):
		rdd_submission = rdd_submission.union(sc.parallelize([('problem_id', problem_id)]))
		rdd_submission = rdd_submission.union(sc.parallelize([('problem_display_name', problem_display_name)]))
		rdd_submission = rdd_submission.union(sc.parallelize([('attempt_category', attempt_category)]))	
		output_key = (course_id, answer_id)
		output_value = (timestamp, rdd_submission.collect())
		result.append((output_key, output_value))

	
	if len(rdd_event.lookup('submission')) > 0:	#'submission' key is present under the event key of the log file
		rdd_submissions = sc.parallelize(rdd_event.lookup('submission')[0].items())
		for answer_id in rdd_submissions.keys().collect():
			if not is_hidden_answer(answer_id):
				rdd_submission = sc.parallelize(rdd_submissions.lookup(answer_id)[0].items())
				answer_value = rdd_answers.lookup(answer_id)[0]
				if answer_value != rdd_submission.lookup('answer')[0]:
					rdd_submission = rdd_submission.union(sc.parallelize([('answer_value_id', answer_value)]))  #answer_value_id is not currently present under the submission key
				
				append_submission(answer_id, rdd_submission)
	else:
		rdd_answers = rdd_event.lookup('answers')
		rdd_correct_map = rdd_event.lookup('correct_map')
		for answer_id in rdd_answers.toLocalIterator():
			if not is_hidden_answer(answer_id):
				answer_value = rdd_answers.lookup('answer_id')
				
				if answer_id not in rdd_correct_map.toLocalIterator():
					f = open('error.txt','a')
					f.write("Unexpected answer_id %s not in correct_map: %s" % (answer_id, event))
					f.close()
					continue
				correctness = sc.parallelize(rdd_correct_map.lookup('answer_id')[0].items()).lookup('correctness') == 'correct'
				submission = {
					'answer_value_id' : answer_value,
					'correct' : correctness,
				}
				rdd_submission = sc.parallelize(submission.items())
				append_submission(answer_id, rdd_submission)

	return result
	
def is_hidden_answer(answer_id):          #answers that are not graded are categorised under hidden_answers. Students can attemp these 						questions, but these will not be evaluated.So, no distribution will be required. 
	if answer_id.endswith('_dynamath'):
		return True
	
	if answer_id.endswith('_comment'):
		return True

	return False

if __name__ == "__main__":
	inp = sc.textFile(sys.argv[1]).map(lambda x : eval(x))
	tmp = inp.groupByKey().collect()
        for i in range(len(tmp)):

#		print("**********************************************************************")
#		print tmp[i][0] , "and" ,  tmp[i][1]
                ProblemCheckEventMixin(tmp[i][0], tmp[i][1])	#input key : (problem_id, username)
								#input value : iterator of (timestamp, problem_check_info)
