package code.querying;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class randomaccesstest {

	public static void main(String[] args) throws IOException{
		//Configuration conf = getConf();
		Configuration conf = new Configuration();
		WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
		f.loadIndex(new Path("/user/hadoop04/wiki2.findex.dat"), new Path("/user/hadoop04/wiki2.dat"), FileSystem.get(conf));
		File file = new File("test_output.txt");
		file.createNewFile();
		FileWriter fw = new FileWriter(file);
		
		WikipediaPage page;

		// fetch docno
		page = f.getDocument(1000);
		System.out.println(page.getDocid() + ": " + page.getTitle());
		System.out.println(page.getContent());
		fw.write(page.getTitle());
		fw.write(page.getContent());
		
		fw.close();
	}

}
