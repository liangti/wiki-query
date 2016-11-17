Query1 is a mapreduce job that takes as input the full inverted index and a query and filters the inverted index based on the query. If the query contains more than one word, the whole query should be surrounded by quotation marks. The following is an example command line call to Query1.

bin/hadoop jar nameofjarfile.jar code.querying.Query1 "hello and goodbye"

This command would create an output file with the lines from the inverted index that correspond to the words "hello" and "goodbye".

Query2 is not a mapreduce job. Query2 takes as input the reduced inverted index (result from Query1) and writes to the file "queryoutput.txt" an index of the appropriate documents from a given query and a list of the words from the query and their offsets. The following is an example command line call to Query2.

java -cp  nameofjar.jar code.querying.Query2 "path/to/input/file" "hello or goodbye"

If the input file created from the inverted index looked like this:

hello <doc1#1,4,5>,<doc2#2,3,4>,<doc3#5,6>
goodbye <doc2#8,9>,<doc3#6,10,11>

The output file "queryoutput.txt" would be:

doc1  <hello#1,4,5>
doc2  <hello#2,3,4>,<goodbye#8,9>
doc3  <hello#5,6>,<goodbye#6,10,11>

If the input were the same, but instead the query was "hello and goodbye", the "queryoutput.txt" file would contain the following.

doc2  <hello#2,3,4>,<goodbye#8,9>
doc3  <hello#5,6>,<goodbye#6,10,11>

Currently the querys are processed left to right, so the query "a and b or c and d" is processed as (((a and b) or c) and d). 

##
TODO:
Need to deal with querying for words not in inverted index. Currently this will cause a null pointer exception.
