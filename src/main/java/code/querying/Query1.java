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

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
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
				String n = s;
				if (n.equalsIgnoreCase(word.toString())){
					context.write(word, line);
				}
			}
			//if (word.equals(Query)){
			//	context.write(new Text(word), new StringIntegerVector("temp2", temp1));
			//}
		}
	}

//	public static class Query1reducer extends
//			Reducer<Text, StringIntegerVector, Text, StringIntegerVector> {
//		public String Query;
//		protected void setup(Reducer<Text, StringIntegerVector, Text, StringIntegerVector>.Context context)
//				throws IOException, InterruptedException {
//			super.setup(context);
//			Configuration conf = context.getConfiguration();
//			String param = conf.get("query");
//			this.Query = param;
//		}
//		@Override
//		public void reduce(Text word, Iterable<StringIntegerVector> articlesAndPos, Context context)
//				throws IOException, InterruptedException {
//			
//            context.write(word, value);
//		}
//	}

	public static void main(String[] args) throws Exception{
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
	    String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    conf.set("query", extraArgs[2]);
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Query1.class);
	    job.setMapperClass(Query1mapper.class);
//	    job.setReducerClass(Query1reducer.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    
	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}