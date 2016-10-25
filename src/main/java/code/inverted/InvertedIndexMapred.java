package code.inverted;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

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

import code.lemma.LemmaIndexMapred;
import code.lemma.LemmaIndexMapred.LemmaIndexMapper;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.StringIntegerList.StringIntegerVector;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringIntegerVector> {

		@Override
		public void map(Text articleTitle, Text indices, Context context) throws IOException,
		InterruptedException {
		// TODO: You should implement inverted index mapper here
			StringIntegerList indincesList=new StringIntegerList();
			System.out.print(articleTitle.toString());
			System.out.println("~~~~~~");
			System.out.print(indices.toString());	
			indincesList.readFromString(indices.toString());	
			
			List<StringIntegerVector> liv=indincesList.getIndices();
			for(StringIntegerVector itr : liv){
				String title=articleTitle.toString();
				String word=itr.getString();
				Vector<Integer> position=itr.getValue();
				StringIntegerVector invert=new StringIntegerVector(title,position);
				context.write(new Text(word), invert);
			}
			
			
			
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
			Reducer<Text, StringIntegerVector, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringIntegerVector> articlesAndPos, Context context)
				throws IOException, InterruptedException {
				// TODO: You should implement inverted index reducer here
                    HashMap<String,Vector<Integer>> invertedMap=new HashMap<String,Vector<Integer>>();
                    StringIntegerList value=new StringIntegerList();
                    for(StringIntegerVector itr: articlesAndPos){
                    	value.add(itr);
//                    	String title=itr.getString();
//                    	Vector<Integer> position=itr.getValue();
//                    	invertedMap.put(title,position);
        
                    }
//                    StringIntegerList value=new StringIntegerList(invertedMap);
                    
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
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(InvertedIndexMapred.class);
	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(StringIntegerVector.class);
	    

	    //job.setOutputFormatClass(OutputFormat.class);	 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
