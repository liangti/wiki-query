package code.querying;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryProcessor {
	
	HashMap<String, HashSet<String>> results = new HashMap<String, HashSet<String>>();
	String goal;
	
	public QueryProcessor(String filename, String query) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		try {
			String line = br.readLine();
		    while (line != null) {
		    	String[] pieces = line.split("\t");
		    	HashSet<String> articles = new HashSet<String>();
		    	Pattern p = Pattern.compile("<(.*?)#");
		    	Matcher m = p.matcher(pieces[1]);
		    	while (m.find()){
		    		articles.add(m.group(1));
				}
		    	results.put(pieces[0], articles);
		    	line = br.readLine();
		    }
		} finally {
		    br.close();
		}
		goal = query.replaceAll("\\(|\\)", "//");
	}
	public void processQuery(String query){
		while (!results.containsKey(goal)){
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
		return newQuery; //return the new query with // around the processed part
	}
	private String getSubquery(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	private String generateNewQuery(String query, String subquery) {
		// TODO Auto-generated method stub
		return null;
	}
	public void processLtoR(String subQuery){
		
	}
	
 	public HashSet<String> combineOR(String w1, String w2){
 		HashSet<String> w1Articles = results.get(w1);
 		HashSet<String> w2Articles = results.get(w2);
 		HashSet<String> w1copy = (HashSet<String>) w1Articles.clone();
 		w1copy.addAll(w2Articles);
 		return w1copy;
 	}
 	//method to combine two sets on "and"
 	public HashSet<String> combineAND(String w1, String w2){
 		HashSet<String> w1Articles = results.get(w1);
 		HashSet<String> w2Articles = results.get(w2);
 		HashSet<String> w1copy = (HashSet<String>) w1Articles.clone();
 		w1copy.retainAll(w2Articles);
 		return w1copy;
 	}
}
