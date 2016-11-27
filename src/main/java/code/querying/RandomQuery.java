package code.querying;

import java.io.IOException;
import java.util.HashMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;



public class RandomQuery {
	public static class RandomMapper extends Mapper<Text, Text, Text, Text> {

		String query;
		Pattern p = Pattern.compile("<([^<>]*)>");
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			Configuration conf=context.getConfiguration();
			//Text out=DefaultStringifier.load(context.getConfiguration(), "query_word", Text.class);
			
			query=conf.get("query_word");
			
			
		}
		@Override
		
		
		
		public void map(Text word, Text indices, Context context) throws IOException,
		InterruptedException {
		// TODO: You should implement inverted index mapper here
//			System.out.println(word+"word");
			String token=word.toString();
			if(!token.equals(query))return;
			
			Matcher m = p.matcher(indices.toString());
			
			while (m.find()) {
				//System.out.println(m.group(1)+"List_read");
				String[] readline = m.group(1).split("#");
				
		        context.write(new Text(readline[0]), new Text(readline[1]));
			}
		}
		}

	public static class RandomReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text DocID, Iterable<Text> WordAndPos, Context context)
				throws IOException, InterruptedException {
			Map<String,String> map=new HashMap<String,String>();
			for(Text itr:WordAndPos){
				String[] posArray=itr.toString().split(",");
				for(int i=0;i<posArray.length;i++)
					map.put(posArray[i], "");
			}
			
			Configuration conf = context.getConfiguration();
			WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
			Path p1=new Path("/user/hadoop04/lt/forward_index/wiki.findex.dat");
			Path p2=new Path("/user/hadoop04/lt/forward_index/wiki.dat");
			f.loadIndex(p1, p2,FileSystem.get(conf));
			WikipediaPage page;
			
			int id=Integer.parseInt(DocID.toString());
			// fetch docno
			page = f.getDocument(id);
			//System.out.println(page.getDocid() + ": " + page.getTitle());
			
			String content=page.getContent();
			Text key=new Text(page.getTitle());
			
			for(String mapKey:map.keySet()){
				int begin=Integer.parseInt(map.get(mapKey));
				Text value=new Text(content.substring(begin,begin+8));
				context.write(key, value);
			}
			
			
		}
	}

	public static void main(String[] args) throws Exception{
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
	    
	    GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		String[] otherArgs=gop.getRemainingArgs();
		conf.set("query_word", otherArgs[2]);
		
		
		Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(RandomQuery.class);
	    job.setMapperClass(RandomMapper.class);
	    
	    job.setReducerClass(RandomReducer.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    

	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
