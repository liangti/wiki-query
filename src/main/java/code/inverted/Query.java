package code.inverted;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.jar.JarFile;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import code.lemma.LemmaIndexMapred;
import code.lemma.LemmaIndexMapred.LemmaIndexMapper;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import util.StringIntegerList.StringInteger;
import util.StringIntegerList.StringIntegerArray;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class Query {
	public static class QueryMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		String invert_word;
		String invert_index;
	    StringIntegerList query_index;
	    Map<String, ArrayList<Integer>> query_map;
	    
	    
		protected void setup(Context context)throws IOException,InterruptedException
	    {
			super.setup(context);
			Path peoplePath = new Path ("test.txt");
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(peoplePath)));//creates a buffered reader for the people.txt file
			String line;
			while((line = br.readLine()) != null){
				String[] cur=line.split("\t");
				System.out.println(cur[0]);
				if(cur[0].equals("Morty")){
					System.out.println("hit");
					invert_word=cur[0];
					invert_index=cur[1];
				}
			}
			br.close();
			query_index=new StringIntegerList();
			query_index.readFromString(invert_index);
			
			query_map=query_index.getMap();
			for(String itr:query_map.keySet())System.out.println("map\t"+itr+" "+query_map.get(itr).size());
	    }
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
		InterruptedException {
		// TODO: You should implement inverted index mapper here
			String title=page.getTitle();
			String content=page.getContent();
			System.out.println("mapper"+" "+query_map.size());
			if(query_map.containsKey(title)){
				Text key=new Text();
				Text value=new Text();
				key.set(title);
				ArrayList<Integer> index=query_map.get(title);
				System.out.println("Yes***********title");
				for(int i=0;i<index.size();i++){
					int position=index.get(i);
					value.set(content.substring(position,position+invert_word.length()));
					System.out.println(title+"\t"+content.substring(position,position+invert_word.length()));
					context.write(key, value);
				}
			}
			
			
		}	
	}

	public static class QueryReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text lemma, Iterable<Text> articlesAndPos, Context context)
				throws IOException, InterruptedException {
				// TODO: You should implement inverted index reducer here
                    
				}
	}

	public static void main(String[] args) throws Exception{
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Query.class);
	    job.setMapperClass(QueryMapper.class);
	    
	    job.setReducerClass(QueryReducer.class);
	    job.setInputFormatClass(WikipediaPageInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.addCacheFile(new Path("/etc/test.txt").toUri());//adds people.txt to cache
	    
	    
	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
