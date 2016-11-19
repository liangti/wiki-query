package code.querying;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;
import code.lemma.Tokenizer;

/**
 * 
 *
 */
public class Query {
	public static class QueryMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here
			Tokenizer tokenier=new Tokenizer();
			Text title=new Text();
			String articletitle=page.getTitle();
			title.set(articletitle);
			String passage=page.getContent();
			
	 	    
	 		   
			context.write(title,new Text(" "));	

		}
	}
	
	public static void main(String[] args) throws Exception{
		
		 Configuration conf = new Configuration();
		 GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		 String[] otherArgs=gop.getRemainingArgs();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Query.class);
		    job.setMapperClass(QueryMapper.class);
		    
		    job.setInputFormatClass(WikipediaPageInputFormat.class);
		    
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
		    

		    //job.setOutputFormatClass(OutputFormat.class);	 
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

