package code.querying;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import code.lemma.Tokenizer;

/**
 * 
 *
 */
public class Query {
	public static class QueryMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		private String word="";
		private StringIntegerList word_list=new StringIntegerList(); 
		private Map<String, ArrayList<Integer>> indiceMap=new HashMap<String, ArrayList<Integer>>();
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			Configuration conf=context.getConfiguration();
			//Text out=DefaultStringifier.load(context.getConfiguration(), "query_word", Text.class);
			
			
			Text out=new Text(conf.get("query_word"));
			String query_word=out.toString();
			
			
			Path path = new Path ("part-r-00000");
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String str;
			
			while((str = br.readLine()) != null){
				String[] line=str.split("\t");
				if(line[0].equals(query_word)){
					word=line[0];
					word_list.readFromString(line[1]);
					indiceMap=word_list.getMap();
					break;
				}
			}
			br.close();
			
		}
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here
			if(page.isEmpty())return;
			
			String id=page.getDocid();
			String content=page.getContent();
			Text key=new Text(word);
			if(indiceMap.containsKey(id)){
				
				ArrayList<Integer> position=indiceMap.get(id);
				
				for(int i=0;i<position.size();i++){
					int begin=position.get(i);
					int end=begin+word.length();
					int head=begin>10?begin-10:0;
					int tail=end<content.length()-10?end+10:content.length()-1;
					String highlight=content.substring(head,tail);
					Text value=new Text(highlight);
					context.write(key, value);
					}
			}

		}
	}
	
	public static void main(String[] args) throws Exception{
		
		 Configuration conf = new Configuration();
		 GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		 String[] otherArgs=gop.getRemainingArgs();
		 Text in = new Text(otherArgs[2]);
		    //DefaultStringifier.store(conf,in,"query_word");
		    conf.set("query_word", in.toString());
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Query.class);
		    job.setMapperClass(QueryMapper.class);
		    job.setInputFormatClass(WikipediaPageInputFormat.class);
		    
		    
		 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		    
			job.addCacheFile(new Path("/user/hadoop04/lt/invert/part-r-00000").toUri());
		    //job.setOutputFormatClass(OutputFormat.class);	 
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

