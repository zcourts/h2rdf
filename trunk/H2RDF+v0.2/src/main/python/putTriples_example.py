import apiCalls

if __name__ == '__main__':
	triples = ''
	count=0
	for i in range(10000):
		triples += '<http://www.Department0.University0.edu/GraduateStudent{0}> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> <http://www.Department0.University0.edu/Course2> . \n'.format(i)
		count += 1
		if count >= 1000 :
			#send 1000 triples 
			apiCalls.bulkPutTriples('ARCOMEMDB_test2', triples)
			triples =''
			count=0   
	#load all the triples to the specified table iusing MapReduce 
	apiCalls.bulkLoadTriples('ARCOMEMDB_test2')
