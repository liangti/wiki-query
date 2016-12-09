package code.querying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/*
 * This is the final step in querying
 * This class takes as input the path to the docid	<word#offsets> file 
 * generated from Query2 and outputs an html formated list of documents
 * author: Kelley
 */

public class GetDocs {
	public static void main(String[] args) throws IOException{
		//first arg (after queueing flag) is file path
		Configuration conf = new Configuration();
		WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
		File tempfile = new File("temp.txt");
		FileWriter bw = new FileWriter(tempfile);
		f.loadIndex(new Path("/user/hadoop04/lt/forward_index/wiki.findex.dat"), new Path("/user/hadoop04/lt/forward_index/wiki.dat"), FileSystem.get(conf));
		String filepath = args[1];
		String query = args[2];
		
		QueryProcessor qp = new QueryProcessor(filepath, query);
		qp.processQuery(query);
		String results = qp.getResults();
		Scanner scanner = new Scanner(results);
		
		WikipediaPage page;
		Pattern p = Pattern.compile("<(.*?)#(.*?)>");
		int count = 0;
		String line = scanner.nextLine();
		while (scanner.hasNext()){
			if (count > 30){
				line = scanner.nextLine();
				count ++;
				continue;
			}
			count ++;
			String[] pieces = line.split("\t");
			if (pieces.length < 2){
				line = scanner.nextLine();
				continue;
			}
			String docid = pieces[0];
			String wordsandoffsets = pieces[1];
			page = f.getDocument(docid);
			if (page == null){
				line = scanner.nextLine();
				continue;
			}
			bw.write("<title>" + page.getTitle() + "</title>\n");
			bw.write("<content>" + page.getContent() + "</content>\n");
			bw.write("<positions>");
			
			wordsandoffsets.replaceAll(">,<", "><");
			String[] searchList = {"<", ">"};
			String[] replacementList = {"<word>", "</word>"};
			
			wordsandoffsets = StringUtils.replaceEach(wordsandoffsets, searchList, replacementList);
			//words and offsets looks like//
			// <word#offsets>,<word#offsets> 
			//replace open < with <word>
			//replace close > with </word>
			//replace , with ""
			bw.write(wordsandoffsets);

			bw.write("</positions>");
			line = scanner.nextLine();
		}
		bw.write("<count>" + String.valueOf(count) + "</count>");
		bw.flush();
		bw.close();
		scanner.close();
		
	}

}
