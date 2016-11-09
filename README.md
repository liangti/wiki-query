# wiki-query

wiki-query

This maven project is based on framework of bigdata assignment2. I modified the following parts.

The first thing is StringInteger. Since in wiki query, we have to store the position of each words, so the structure is no longer useful. I modified the structure as . In detail, since there are comma between Integer, I used "#" to seperate String and Integer, like this , and create a new data Structure called StringIntegerArray, which contains two things, String and ArrayList. (Refer to the code in StringInteger.java)

The second thing is to capture the character index instead of tokenizing index. So the change is in Tokenizer.java. I firstly did tokenize to transfer Content String into List, and then, used List to match the substring in Content, this could be a slowly way, but I think that the speed of building index is not a matter?

The algorithm should be like this:

List: I am a boy (list_index) | | | | String: I am a boy...(doc_index)

str=List.get(list_index)

if(str.equal(String.substring(doc_index,doc_index+str.length())-> list_index++;doc_index+=str.length(); else doc_index++;

Note: I firstly replace all the punctuations with space, I don't know whether it is correct: sentence = sentence.replaceAll("\pP|\pS"," ");

Here is the problem, I output the "barrier" words, which means I cannot find the word in the raw String, and that acts like a barrier, so that we cannot get the position of the words after this barrier word. Here is an example(I create it):

List: I am a bay ... String: I am a boy ...

So the loop stop at "bay" because this word is different from the word in raw file. I don't know what cause the word in tokenizing list different from the raw word.
