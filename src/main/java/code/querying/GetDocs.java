package code.querying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
		BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
		f.loadIndex(new Path("/user/hadoop04/lt/forward_index/wiki.findex.dat"), new Path("/user/hadoop04/lt/forward_index/wiki.dat"), FileSystem.get(conf));
		String filepath = args[1];
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		String line = br.readLine();
		WikipediaPage page;
		Pattern p = Pattern.compile("<(.*?)#(.*?)>");
		while (line != null){
			String[] pieces = line.split("\t");
			String docid = pieces[0];;
			String wordsandoffsets = pieces[1];
			page = f.getDocument(docid);
			if (page == null){
				line = br.readLine();
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
			
			Matcher m = p.matcher(wordsandoffsets);
			String word = m.group(1);
			String offsets = m.group(2);
			
			String[] offsetList = offsets.split(",");
			bw.write("<test>");
			for (String off:offsetList ){
				int offsetint = Integer.parseInt(off);
				String incontext = page.getContent().substring(offsetint, word.length());
				bw.write(incontext);
			}
			bw.write("</test>");
			
			line = br.readLine();
		}
		bw.flush();
		bw.close();
		br.close();
		
	}

}
