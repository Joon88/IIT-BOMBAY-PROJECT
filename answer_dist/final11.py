from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('my_app2').setMaster('local')
sc = SparkContext(conf = conf)

import sys

def AnswerDistributionPerCourseMixin(key, values):
	rdd_key = sc.parallelize(key)
	rdd_values = sc.parallelize([x for x in values]).sortByKey()
	
	add_metadata_to_answer(rdd_key.collect()[1], rdd_values.collect()[-1][1])
	
	if not should_include_answer(rdd_values.collect()[-1][1]):
		return
	rdd_most_recent_answer = sc.parallelize(rdd_values.collect()[-1][1])
	problem_id = rdd_most_recent_answer.lookup('problem_id')[0]
	problem_display_name = rdd_most_recent_answer.lookup('problem_display_name')[0]
	answer_uses_value_id = ('answer_value_id' in rdd_most_recent_answer.keys().collect())  #it gives either true or false
	answer_dist = {}
	
	def func0(answer_id, y, problem_display_name):
		rdd_answer = sc.parallelize(y)
		add_metadata_to_answer(rdd_key.collect()[1], rdd_answer.collect())	
		answer_grouping_key = get_answer_grouping_key(rdd_answer)
		
		if answer_grouping_key not in answer_dist:
			if answer_uses_value_id:
				value_id = rdd_answer.lookup('answer_value_id')[0] if 'answer_value_id' in rdd_answer.keys().collect() else ''
				answer_value = rdd_answer.lookup('answer')[0] if 'answer' in rdd_answer.keys().collect() else ''
			else:
				value_id = ""
				answer_value = rdd_answer.lookup('answer')[0] if 'answer' in rdd_answer.keys().collect() else rdd_answer.lookup('answer_value_id')[0]
#			value_id = stringify(value_id)
#			answer_value_contains_html = (value_id is not None and value_id != '')
#			answer_value = stringify(answer_value, contains_html = answer_value_contains_html)

			question = rdd_answer.lookup('question')[0] if 'question' in rdd_answer.keys().collect() else ''
			
			answer_dist[answer_grouping_key] = {
		            'ModuleID': problem_id,
                	    'PartID': answer_id,
                   	    'ValueID': value_id or '',
                    	    'AnswerValue': answer_value or '',
                    	    'Variant': 1,
                    	    'Problem Display Name': problem_display_name or '',
                    	    'Question': question,
                    	    'Correct Answer': '1' if rdd_answer.lookup('correct') else '0',
                    	    'First Response Count': 0,
                    	    'Last Response Count': 0,
			}
	        
	                

		attempt_category = rdd_answer.lookup('attempt_category')[0]
		           		

		if attempt_category == 'first':
                	answer_dist[answer_grouping_key]['First Response Count'] += 1
            	elif attempt_category == 'last':
                	answer_dist[answer_grouping_key]['Last Response Count'] += 1
		else:	
			print "**************************************************************************************"
			print "Unknown attempt category"
			print "**************************************************************************************"	

	rdd_values = rdd_values.sortByKey(ascending = False)  #.map(lambda x, y : func0(y))
	rdd_values = rdd_values.map(func0(rdd_key.collect()[1], rdd_values.collect()[0][1], problem_display_name))

	f = open(sys.argv[2],'a')		
	for answer_entry in answer_dist.values():
		f.write(rdd_key.collect()[0] + ',' + str(answer_entry.items()) + '\n')
	f.close()	

def get_answer_grouping_key(rdd_answer):
	if 'answer_value_id' in rdd_answer.keys().collect():
		answer_value = rdd_answer.lookup('aniswer_value_id')
	else:
		answer_value = rdd_answer.lookup('answer')

	return '{value}_{variant}'.format(value = answer_value, variant = 1)

def add_metadata_to_answer(answer_id, answer):
	
	rdd_answer = sc.parallelize(answer)
	
	rdd_answer_metadata = sc.parallelize(sc.parallelize(sc.textFile('answer_metadata.txt').map(lambda x : eval(x)).collect()[0].items()).lookup(answer_id)[0].items())
	if len(rdd_answer_metadata.collect()) > 0:
		def func(x, y):
                	if len(rdd_answer.lookup(x)) == 0 and isinstance(value, basestring):
                        	rdd_answer = rdd_answer.union(sc.parallelize([(x, y)]))


		
		rdd_answer_metadata = rdd_answer_metadata.map(func)
		
		if 'answer' not in rdd_answer.keys().collect():
			response_type = rdd_answer.lookup('response_type')[0]
			if response_type in ['choiceresponse', 'multiplechoiceresponse']:
				if 'answer_value_id_map' in rdd_answer_metadata.keys().collect():
					answer_value_id = rdd_answer.lookup('answer_value_id')[0]
					rdd_answer_value_id_map = sc.parallelize(rdd_answer_metadata.lookup('answer_value_id_map')[0].items())

					def get_answer_value(code):
						return rdd_answer_value_id_map.lookup(code)[0]
					if isinstance(answer_value_id, basestring):
						rdd_answer = rdd_answer.union(sc.parallelize([('answer',get_answer_value(answer_value_id))]))
					elif isinstance(eval(answer_value_id), list):
						rdd_answer = rdd_answer.union(sc.parallelize([get_answer_value(code) for code in answer_value_id]))
			else:
				def func1(x, y):
					if x == 'answer':
						y = rdd_answer.lookup('answer_value_id')
				
				rdd_answer = rdd_answer.map(lambda x, y : func1)
				rdd_answer = rdd_answer.filter(lambda x, y : x != 'answer_value_id') 
		

def should_include_answer(answer):
	rdd_answer = sc.parallelize(answer)
	response_type = rdd_answer.lookup('response_type')[0]
	if response_type is None:
		return False

	list_of_valid_response_types = ['multiplechoiceresponse','choiceresponse']
	
	if response_type in list_of_valid_response_types:
		return True
	
	return False

if __name__ == "__main__":
	inp = sc.textFile(sys.argv[1]).map(lambda x : eval(x))
        tmp = inp.groupByKey().collect()
        for i in range(len(tmp)):
                AnswerDistributionPerCourseMixin(tmp[i][0], tmp[i][1])    #input key : (course_id, answer_id)
                                                                	  #input value : iterator of (timestamp, answer_data)

