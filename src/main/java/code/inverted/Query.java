package code.inverted;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;


/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class Query {
	public static class QueryMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			String title=inputPage.getTitle();
		if(title.equals("Noise pollution")){
			String str=inputPage.getContent();
			System.out.println(str.substring(472,485));
			Text key=new Text();
			Text value=new Text();
			key.set(title);
			value.set(str.substring(472,485));
			context.write(key, value);
		}
		}
	}

	public static void main(String[] args) throws Exception{
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(InvertedIndexMapred.class);
	    job.setMapperClass(QueryMapper.class);
	    
	    job.setInputFormatClass(WikipediaPageInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    

	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
