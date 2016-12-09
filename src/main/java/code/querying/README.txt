Query1 is a mapreduce job that takes as input the full inverted index and a query and filters the inverted index based on the query. If the query contains more than one word, the whole query should be surrounded by quotation marks. The following is an example command line call to Query1.

bin/hadoop jar nameofjarfile.jar code.querying.Query1 "hello and goodbye"

The second stage of query processing is in "getDocs". GetDocs creates a query processor object with the filepath to the filtered inverted index and the query. It then processes the query. Calling getResults on the query processor object returns a string in which each line is a document and the words it contains with their offsets. 

GetDocs uses this string, along with the random access functionallity provided by Cloud9 to create an xml file of query results. 
