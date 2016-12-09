package code.querying;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/*
 * This is the first step in querying
 * This mapreduce job takes as input the inverted index and produces a filtered inverted index
 * with only the word that are in the query
 * author: Kelley + Ti
 */
public class Query1{
	public static class Query1mapper extends Mapper<Text, Text, Text, Text> {
		public String Query;
		public List<String> queryWords;
		
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String param = conf.get("query");
			Query = param;
			Query = Query.replaceAll("\\(|\\)", "");
			queryWords = new ArrayList<String>(Arrays.asList(Query.split(" ")));
			for (int i=0;i<queryWords.size()-1;i++){
				String word = queryWords.get(i);
				word = word.trim();
				queryWords.set(i, word);
			}
		}
		@Override
		public void map(Text word, Text line, Context context) throws IOException,
		InterruptedException {
			for (String s:queryWords){
				if ((!s.equalsIgnoreCase("and")) && (!s.equalsIgnoreCase("or")) && (!s.equalsIgnoreCase("not"))){
					if (s.equalsIgnoreCase(word.toString())){
						context.write(word, line);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

	    String[] extraArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    conf.set("query", args[3]);
	    
	    
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Query1.class);
	    job.setMapperClass(Query1mapper.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    String Query=args[3];
	    Query = Query.replaceAll("\\(|\\)", "");
	    ArrayList<String> queryWords = new ArrayList<String>(Arrays.asList(Query.split(" ")));
		for (int i=0;i<queryWords.size()-1;i++){
			String word = queryWords.get(i);
			word = word.trim();
			queryWords.set(i, word);
		}
	    String pathPrefix=args[1];
	    for (String s:queryWords){
	    	if ((s.equalsIgnoreCase("and")) || (s.equalsIgnoreCase("or")))continue;
	    	char head=s.charAt(0);
			int index=0;
			if((head>='a'&&head<='z')) index=head-'a';
			else if(head>='A'&&head<='Z')index=head-'A';
			else index=26;
	    	String inputPath=pathPrefix+"/"+String.valueOf(index);
	    	FileInputFormat.addInputPath(job, new Path(inputPath));
	    }
	    
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}