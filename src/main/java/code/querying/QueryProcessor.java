package code.querying;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryProcessor {
	
	HashMap<String, HashSet<String>> results = new HashMap<String, HashSet<String>>();
	static HashMap<String, String> fileLines = new HashMap<String, String>();
	HashSet<String> all_docs = new HashSet<String>();
	HashSet<String> not_words = new HashSet<String>();
	String goal;
	
	
	public QueryProcessor(String filename, String query) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		try {
			String line = br.readLine();
		    while (line != null) {
		    	String[] pieces = line.split("\t");
		    	fileLines.put(pieces[0], pieces[1]);
		    	HashSet<String> articles = new HashSet<String>();
		    	Pattern p = Pattern.compile("<(.*?)#");
		    	Matcher m = p.matcher(pieces[1]);
		    	while (m.find()){
		    		articles.add(m.group(1));
		    		all_docs.add(m.group(1));
				}
		    	results.put(pieces[0], articles);
		    	line = br.readLine();
		    }
		} finally {
		    br.close();
		}
		goal = query.replaceAll("\\(|\\)", " ").trim().replaceAll("\\s+", "_");
	}
	
	public void processQuery(String query){
		String tempgoal = query.replaceAll("\\(|\\)", " ").trim().replaceAll("\\s+", "_");
		while (!results.containsKey(tempgoal)){
			if (query.contains("(")){
				query = processSubquery(query);
			}else{
				processLtoR(query);
			}
		}
	}
	public String processSubquery(String query){
		String subquery = getSubquery(query); //finds the first part surrounded by parens
		processQuery(subquery);
		String newQuery = generateNewQuery(query, subquery);// replace the subquery part of query with //subquery//
		return newQuery; //return the new query with _'s in the processed part
	}
	
	private String getSubquery(String query) {
		//treats the query as a character array and finds the sub array surrounded by matching parentheses
		char[] queryCharArray = query.toCharArray();
		int openParen = 0;
		int indexnum = 0;
		int startIndex = 0;
		int endIndex = 0;
		for (char c:queryCharArray){
			if (c == '('){
				if (openParen == 0){
					startIndex = indexnum;
				}
				openParen++;
			} else if (c == ')'){
				openParen--;
				if (openParen == 0){
					endIndex = indexnum;
					break;
				}
			}
			indexnum++;
		}
		return query.substring(startIndex+1, endIndex);
	}
	
	private String generateNewQuery(String query, String subquery) {
		char[] queryCharArray = query.toCharArray();
		int openParen = 0;
		int indexnum = 0;
		int startIndex = 0;
		int endIndex = 0;
		for (char c:queryCharArray){
			if (c == '('){
				if (openParen == 0){
					startIndex = indexnum;
				}
				openParen++;
			} else if (c == ')'){
				openParen--;
				if (openParen == 0){
					endIndex = indexnum;
					break;
				}
			}
			indexnum++;
		}
		query = query.substring(0, startIndex) + subquery.replaceAll("\\s+", "_" )+ query.substring(endIndex+1);
		return query;
	}
	
	public void processLtoR(String subQuery){

		subQuery = this.replaceNOTS(subQuery);
		String[] queryWords = subQuery.split(" ");
		HashSet<String> result;
		result = initialResult(queryWords[0]);
		int i = 1;
		while (i<queryWords.length){
			if (queryWords[i].equalsIgnoreCase("and")){
				if ((i+1) < queryWords.length){
					result = this.combineAND(result, queryWords[i+1]);
					i = i + 2; //because we processed and and the query word following and
				}else{
					i++; //ignore the and, its the last word in the query
				}
			} else if (queryWords[i].equalsIgnoreCase("or")){
				if ((i+1) < queryWords.length){
					result = this.combineOR(result, queryWords[i+1]);
					i = i + 2; //because we processed or and the query word following and
				}else{
					i++; //ignore the or, its the last word in the query
				}
			} else {
				//implicit "and"
				result = this.combineAND(result, queryWords[i]);
				i++;
			}
		}
		results.put(subQuery.replaceAll(" ", "_"), result);
	}
	
	private String replaceNOTS(String subQuery) {
		while (Arrays.asList(subQuery.split(" ")).contains("not")){
			List queryWords = Arrays.asList(subQuery.split(" "));
			int notIndex = queryWords.indexOf("not");
			HashSet<String> all_docs_copy = (HashSet<String>) all_docs.clone();
			String nextWord = (String) queryWords.get(notIndex+1);
			all_docs_copy.removeAll(results.get(nextWord));
			results.put("not_"+nextWord, all_docs_copy);
			not_words.add(nextWord);
			subQuery = subQuery.replace("not "+nextWord, "not_" +nextWord);
		}
		return subQuery;
	}
	private HashSet<String> initialResult(String string) {
		return results.get(string);
	}
	
	public HashSet<String> combineOR(HashSet<String> result, String w2){
 		HashSet<String> w1Articles = result;
 		HashSet<String> w2Articles = results.get(w2);
 		HashSet<String> w1copy = (HashSet<String>) w1Articles.clone();
 		w1copy.addAll(w2Articles);
 		return w1copy;
 	}
 	//method to combine two sets on "and"
 	public HashSet<String> combineAND(HashSet<String> result, String w2){
 		HashSet<String> w1Articles = result;
 		HashSet<String> w2Articles = results.get(w2);
 		HashSet<String> w1copy = (HashSet<String>) w1Articles.clone();
 		w1copy.retainAll(w2Articles);
 		return w1copy;
 	}
 	public String getOffsets(String article){
		Pattern p = Pattern.compile("<" + article + "#(.*?)>");
		String wordoffsetlist =  "";
		int count = 0;
 		for (String word:fileLines.keySet()){
 			if (not_words.contains(word)){
 				continue;
 			}
 			String docsandoffsets = fileLines.get(word);
	    	Matcher m = p.matcher(docsandoffsets);
	    	if (m.find()){
	    		if (count > 0){
	    			wordoffsetlist += ",";
	    		}
	    		wordoffsetlist += "<" + word + "#" + m.group(1) + ">";
		    	count++;
	    	}
 		}
 		return wordoffsetlist;
 	}
 	public String getResults(){
 		String result = "";
 		String line;
 		
 		for (String article:results.get(goal)){
 			String offsets = getOffsets(article);
 			line = article + "\t" + offsets + "\n";
 			result = result + line;
 		}
 		return result;	
 	}
}
