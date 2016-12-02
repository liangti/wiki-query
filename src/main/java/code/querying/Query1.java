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

import code.lemma.LemmaIndexMapred;
import code.lemma.LemmaIndexMapred.LemmaIndexMapper;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.StringIntegerList.StringIntegerVector;

/*
 * This is the first step in querying
 * This mapreduce job takes as input the inverted index and produces a filtered inverted index
 * with only the word that are in the query
 * author: Kelley
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
			queryWords = new ArrayList<String>(Arrays.asList(Query.split(" ")));
		}
		@Override
		public void map(Text word, Text line, Context context) throws IOException,
		InterruptedException {
			for (String s:queryWords){
				if ((!s.equalsIgnoreCase("and")) && (!s.equalsIgnoreCase("or"))){
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

	    
	  	FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}