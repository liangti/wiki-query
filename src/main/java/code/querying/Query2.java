package code.querying;
/*
 * This is the second step in processing a query
 * it takes as input the filtered inverted index from Query1 and produces an index of
 * docid	<word#offset>
 * author:Kelley
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query2 {
	///word, docs
	static HashMap<String, HashSet<String>> hmap = new HashMap<String, HashSet<String>>();
	//word, docs and offsets as 1 string 
	static HashMap<String, String> fileLines = new HashMap<String, String>();
 	public Query2(String filepath, String query) throws IOException {
 		//System.out.println("building HashMap...");
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		try {
			String line = br.readLine();
		    while (line != null) {
		    	//System.out.println("readingline...");
		    	String[] pieces = line.split("\t");
		    	fileLines.put(pieces[0], pieces[1]);
		    	HashSet<String> articles = new HashSet<String>();
		    	Pattern p = Pattern.compile("<(.*?)#");
		    	Matcher m = p.matcher(pieces[1]);
		    	while (m.find()){
		    		articles.add(m.group(1));
				}
		    	hmap.put(pieces[0], articles);
		    	line = br.readLine();
		    }
		} finally {
		    br.close();
		}
		//System.out.println("HashMap built.");
	}
 	//method to combine two sets on "or"
 	public HashSet<String> combineOR(HashSet<String> curresult, String q2){
 		HashSet<String> q2Articles = hmap.get(q2);
 		curresult.addAll(q2Articles);
 		return curresult;
 	}
 	
 	//method to combine two sets on "and"
 	public HashSet<String> combineAND(HashSet<String> curresult, String q2){
 		HashSet<String> q2Articles = hmap.get(q2);
 		curresult.retainAll(q2Articles);
 		return curresult;
 	}
 	
 	public static HashSet<String> initialResult(String q1){
 		return hmap.get(q1);
 	}
 	
 	public String getOffsets(String article){
		Pattern p = Pattern.compile("<" + article + "#(.*?)>");
		String wordoffsetlist =  "";
		int count = 0;
 		for (String word:fileLines.keySet()){
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
 	
 	//get offsets from list of articles and query and write to file
 	public void writeResultsToFile(String[] queryWords, 
 			HashSet<String> articles) throws UnsupportedEncodingException, FileNotFoundException, IOException{
 			File file = new File("queryoutput.txt");
 			file.createNewFile();
 			FileWriter writer = new FileWriter(file);
 			String line;
 			for (String article:articles){
 				line = article + "\t" + getOffsets(article) + "\n";
 				writer.write(line);
 			}
 			writer.flush();
 			writer.close();
 			
 	}
 	///returns the number of non-boolean words in the query
 	public int getWordCount(String query){
 		int count = 0;
 		for (String w:query.split(" ")){
 			if (!(w.equalsIgnoreCase("not|and|or"))){
 				count++;
 			}
 		}
 		return count;
 	}
 	
	public static void main(String[] args) throws IOException{
		//arg 1 is file path
		//arg 2 is query
		
		Query2 process = new Query2(args[1], args[2]);
		String[] queryWords = args[2].split(" ");
		HashSet<String> result;
		result = initialResult(queryWords[0]);
		int i = 1;
		while (i<queryWords.length){
			if (queryWords[i].equalsIgnoreCase("and")){
				if ((i+1) < queryWords.length){
					result = process.combineAND(result, queryWords[i+1]);
					i = i + 2; //because we processed and and the query word following and
				}else{
					i++; //ignore the and, its the last word in the query
				}
			} else if (queryWords[i].equalsIgnoreCase("or")){
				if ((i+1) < queryWords.length){
					result = process.combineOR(result, queryWords[i+1]);
					i = i + 2; //because we processed or and the query word following and
				}else{
					i++; //ignore the or, its the last word in the query
				}
			} else {
				//implicit "and"
				result = process.combineAND(result, queryWords[i]);
				i++;
			}
		}
		//System.out.println(result.toString());
		
		//write to a file called "queryoutput.txt"
		//the format of this file is:
		//article	<lemma#offset1,offset2>,<lemma2#offset3,offset4>
		//article2	<lemma#offset,offset>,<lemma#offset,offset>
		process.writeResultsToFile(queryWords, result);
		
	}
}