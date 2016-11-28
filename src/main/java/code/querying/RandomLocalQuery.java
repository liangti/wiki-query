package code.querying;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class RandomLocalQuery {

	public static void main(String[] args) throws IOException{
		//Configuration conf = getConf();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String query_word=args[3];
		String inputPath=args[1];		
		FileStatus[] status = fs.listStatus(new Path(inputPath));	
		Pattern p = Pattern.compile("<([^<>]*)>");

		Path outputPath=new Path(args[2]);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, outputPath, Text.class, Text.class);
		
		File file = new File("test_output.txt");
		FileWriter fw = new FileWriter(file);
		
		Map<String,String> map=new HashMap<String,String>();
		
		for (int i=0;i<status.length;i++){
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line;
            while((line=reader.readLine())!=null){
            	String[] split=line.split("\t",2);
				if(split[0]==null || "".equals(split[0]))
					continue;
				
				if(!split[0].equals(query_word))continue;
				
				Matcher m = p.matcher(split[1]);
				
				while (m.find()) {
					//System.out.println(m.group(1)+"List_read");
					String[] readline = m.group(1).split("#");
					String position=String.valueOf(query_word.length())+","+readline[1];
					
					map.put(readline[0], position);
				}
				
            }
		}
		
		WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
		Path p1=new Path("/user/hadoop04/lt/forward_index/wiki.findex.dat");
		Path p2=new Path("/user/hadoop04/lt/forward_index/wiki.dat");
		f.loadIndex(p1, p2,FileSystem.get(conf));
		WikipediaPage page;
		
		
		// fetch docno
		//page = f.getDocument(1000);
		//System.out.println(page.getDocid() + ": " + page.getTitle());
		for(String keyMap:map.keySet()){
			String[] split=map.get(keyMap).split(",");
			int len=Integer.parseInt(split[0]);
			//int id=Integer.parseInt(keyMap);
			page=f.getDocument(keyMap);
			if(page==null)continue;
			String content=page.getContent();
			Text word=new Text(query_word);
			for(int i=1;i<split.length;i++){
				int begin=Integer.parseInt(split[i]);
				String outcontent=content.substring(begin,begin+len);
				writer.append(word, new Text(outcontent));
				fw.write(word+"\t"+outcontent+"\n");
			}
		}
		fw.close();
		writer.close();
	}

}