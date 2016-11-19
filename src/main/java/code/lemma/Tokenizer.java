package code.lemma;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class Tokenizer {

	public static List<String> tokens = null;
	public static List<Pattern> noise = null;

	public static Properties props = null;
	public static StanfordCoreNLP pipeline = null;

	public static HashMap<String,Integer> stopword_list;

	public Tokenizer() {
		tokens = new ArrayList<String>();
		

		props = new Properties();
		props.put("annotators", "tokenize, ssplit");
		pipeline = new StanfordCoreNLP(props);

		init_stopword();
	}

	public List<String> tokenize(String documentText) {


		
		/////
		 List<String> lemmas = new LinkedList<String>();
	     // removes all the non-word tokens such as numbers and dates
		 documentText=noiseFilter(documentText);
		// Create an empty Annotation just with the given text
	     Annotation document = new Annotation(documentText);
	     // run all Annotators on this text
	     pipeline.annotate(document);
	    
	     List<CoreMap> sentences = document.get(SentencesAnnotation.class);
	     String lemma="";
	     // Iterate over all of the sentences found
	     for(CoreMap sentence: sentences) {
	         // Iterate over all tokens in a sentence
	         for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
	             //lemmatizes the token
	        	  //lemma=token.get(LemmaAnnotation.class);
	        	  //lemma=lemma.toLowerCase();
	        	  //adds it to the lemma list if its not a stop word
	        	 //if(!stopword_list.containsKey(lemma)){}
	        	lemma = token.get(TextAnnotation.class);	
	 			lemmas.add(lemma);
	 			
	            
	         }
	     }
	     return lemmas;
	}

	public void init_stopword(){
		stopword_list=new HashMap<String,Integer>();
		String[] cur_list="a the about above after again against all an and any as at be because before below between both but by cannot could did do down during each few for from further have here he how i if in into it itself let me more he it you most must my myself no nor not of off on once only or other ought our osu . ! ? , ;".split(" ");
		for(String str:cur_list){
			str=str.toLowerCase();
	    	//System.out.println(str);
			stopword_list.put(str, 1);
		}
	}

	public static String noiseFilter(String sentence) {

		sentence = sentence.replaceAll("\\pP|\\pS","");

		return sentence;
	}
	  public List<Integer> position(String documentText,List<String> token){
	    	List<Integer> pos=new ArrayList<Integer>();
	    	int doc_index=0;
	    	int list_index=0;
	    	while(doc_index<documentText.length()&&list_index<token.size()){
	    		String str=token.get(list_index);
	    		
	    		if(doc_index+str.length()>=documentText.length()){
	    			System.out.println("barry\t"+str);
	    			break;
	    		}
	    		String comp=documentText.substring(doc_index, doc_index+str.length());
	    		//System.out.println(str+" "+comp);
	    		
	    		if(comp.equals(str)){
	    			pos.add(doc_index);
	    			doc_index+=str.length();
	    			list_index++;
	    			continue;
	    		}
	    		else{
	    			doc_index++;
	    		}
	    		
	    	}
	    	return pos;
	    }

}