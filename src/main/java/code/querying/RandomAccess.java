package code.querying;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class RandomAccess {

	public static void main(String[] args) throws IOException{
		//Configuration conf = getConf();
		Configuration conf = new Configuration();
		WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
		Path p1=new Path("/user/hadoop04/lt/forward_index/wiki.findex.dat");
		Path p2=new Path("/user/hadoop04/lt/forward_index/wiki.dat");
		f.loadIndex(p1, p2,FileSystem.get(conf));
		WikipediaPage page;

		// fetch docno
		page = f.getDocument(1000);
		System.out.println(page.getDocid() + ": " + page.getTitle());
		
		// fetch docid
		page = f.getDocument("1875");
		System.out.println(page.getDocid() + ": " + page.getTitle());
		
		
	}

}