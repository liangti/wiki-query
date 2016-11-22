package code.lemma;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.jar.JarFile;

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
import util.StringIntegerList.StringIntegerVector;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;
import code.articles.GetArticlesMapred;
import code.lemma.Tokenizer;

/**
 * This MapReduce job needs 2 input files, the original wikipedia dump file and the queryresult file
 * In the setup of the job, we read the queryresult file and create a hashmap of:
 * (docid, <queryword1#offset1,offset2,offset3>)
 * In the map job, we check if the docid is in the keys of of the hashmap. If it is not we skip it.
 * If it is in the hashmap, we write to context ([<queryword1#offsets>,<queryword2#offsets>], documentcontents).
 *
 */
public class getarticles {
	public static class FinalArticles extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		
		//This hashmap holds the documents that need to be returned as the result of the query
		//and the <word#offsets> stringintegervector list 
		HashMap<String, String> docsAndOffsets = new HashMap<String, String>();
		
		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String QUERY_RESULT = "queryresult.txt";
			String line = null;
			//the query result file shouldnt be on hdfs because its standard java, so hopefully this works
			FileReader fr = new FileReader(QUERY_RESULT);
			BufferedReader br = new BufferedReader(fr);
					
			while ((line = br.readLine()) != null){
				String[] pieces = line.split("\t");
				assert(pieces.length == 2); //make sure this has 2 pieces or we need to do something else
				docsAndOffsets.put(pieces[0], pieces[1]);
			}				
			fr.close();
			br.close();
			
			super.setup(context);
		}
		
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {

			String docid = page.getDocid();
			String passage=page.getContent();

			if (docsAndOffsets.containsKey(docid)){
				Text offsets = new Text(docsAndOffsets.get(docid));
				Text content = new Text(passage);
				context.write(offsets, content);
			}
		}
			
	}
	
	public static void main(String[] args) throws Exception{
		
		 Configuration conf = new Configuration();
		 GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		 String[] otherArgs=gop.getRemainingArgs();
		    Job job = Job.getInstance(conf, "final articles");
		    job.setJarByClass(FinalArticles.class);
		    job.setMapperClass(FinalArticles.class);
		    
		    job.setInputFormatClass(WikipediaPageInputFormat.class);
		    
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
		    

		    //job.setOutputFormatClass(OutputFormat.class);	 
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}