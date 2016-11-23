package code.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.StringIntegerList.StringIntegerArray;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringIntegerArray> {

		@Override
		public void map(Text articleTitle, Text indices, Context context) throws IOException,
		InterruptedException {
		// TODO: You should implement inverted index mapper here
			StringIntegerList indincesList=new StringIntegerList();
			System.out.println(articleTitle+"title");
			indincesList.readFromString(indices.toString());	
		    Map<String, ArrayList<Integer>> Sia=indincesList.getMap();
			
		    for(String mapKey:Sia.keySet()){
		    	Text key=new Text();
		    	key.set(mapKey);
		    	String title=articleTitle.toString();
		    	StringIntegerArray value=new StringIntegerArray(title,Sia.get(mapKey));
		    	context.write(key, value);
		    	
		    }
		    
		    
		    
//			for(StringIntegerArray itr : liv){
//				
//				
//				
//				
//				String title=articleTitle.toString();
//				String word=itr.getString();
//				ArrayList<Integer> cur=itr.getValue();
//				ArrayList<Integer> position=new ArrayList<Integer>();
//				for(int i=0;i<cur.size();i++)
//					position.add(cur.get(i));
//				StringIntegerArray invert=new StringIntegerArray(title,position);
//				Text key=new Text();
//				key.set(word);
//				context.write(key, invert);
//				System.out.println(invert.getValue().size()+"mapper"+word);
//			}
//			
			
			
//			List<StringInteger> outputList=indincesList.getIndices();
//			StringInteger Value;
//			Text Key=new Text();	
//			for (StringInteger pair : outputList) {  
//				Key.set(pair.getString());
//			    
//				Value=new StringInteger(articleTitle.toString(),pair.getValue());
//				//	System.out.println(pair.getString()+"~"+pair.getValue());
//				context.write(Key,Value);	  
//		} 
		}
		}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringIntegerArray, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringIntegerArray> articlesAndPos, Context context)
				throws IOException, InterruptedException {
				// TODO: You should implement inverted index reducer here
                    HashMap<String,ArrayList<Integer>> invertedMap=new HashMap<String,ArrayList<Integer>>();
                    //StringIntegerList value=new StringIntegerList();
                    int index=0;
                    //System.out.println(lemma.toString()+"lemma");
                    for(StringIntegerArray itr: articlesAndPos){
                    	String title=itr.getString();
                    	//System.out.println(index+"reducer\t"+title);
                    	index++;
                    	ArrayList<Integer> position=new ArrayList<Integer>();
                    	ArrayList<Integer> cur=itr.getValue();
                    	System.out.println(cur.size()+"size");
                    	for(int i=0;i<cur.size();i++)
                    		position.add(cur.get(i));
                    	invertedMap.put(title,position);
        
                    }
                    StringIntegerList value=new StringIntegerList(invertedMap);
                    
                    context.write(lemma, value);
//					Text Key=lemma;
//					StringIntegerList  Value;
//					HashMap<String,Integer> stringIntegerMap=new HashMap<String,Integer>();
//					for(StringInteger stringInteger:articlesAndFreqs){
//						//	System.out.println(stringInteger.getString()+"~"+stringInteger.getValue());
//						stringIntegerMap.put(stringInteger.getString(),new Integer(stringInteger.getValue()));
//					}	
//					Value=new StringIntegerList(stringIntegerMap);
//					context.write(Key,Value);	
				}
	}

	public static void main(String[] args) throws Exception{
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();
	    
	    GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		 String[] otherArgs=gop.getRemainingArgs();

		Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(InvertedIndexMapred.class);
	    job.setMapperClass(InvertedIndexMapper.class);
	    
	    job.setReducerClass(InvertedIndexReducer.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(StringIntegerArray.class);
	    

	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
