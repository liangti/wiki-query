package code.inverted;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import util.StringIntegerList;
import util.StringIntegerList.StringIntegerArray;
 

public class filter {

	private static void readTxtFile(String filepath) throws IOException{
        try {  
           
            BufferedReader reader=new BufferedReader(new FileReader(filepath));
            PrintWriter out=new PrintWriter("/Users/uuisafresh/Documents/workspace/wiki/src/main/java/code/inverted/count.txt"); 
            
            
            ArrayList<String> name=new ArrayList<String>();
            int num=0;
            long start=System.currentTimeMillis();
            String str;
            while((str = reader.readLine())!=null){   
            	String[] line=str.split("\t");
            	name.add(line[0]);
            	StringIntegerList indincesList=new StringIntegerList();
            	indincesList.readFromString(line[1]);
            	List<StringIntegerArray> list=indincesList.getIndices();
            	int count=0;
            	for(StringIntegerArray itr:list){
            		count+=itr.getValue().size();
            	}
            	int doc_count=list.size();
            	//out.write(name.get(num)+"\t"+count+"\t"+doc_count+"\n");
            	if(line[0].equals("Farquhar")){
            		System.out.println(line[0]+" "+line[1]);
            		break;
            	}
                num++;
                if(num%10000==0)System.out.println(num);
            }  
            reader.close();
            out.close();
            long end=System.currentTimeMillis();
            System.out.println(num+" "+(end-start)+" "+name.size());
            for(int i=0;i<name.size();i++){
            	out.write(name.get(i));
            }
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        }  
    }  
     
    public static void main(String argv[]) throws IOException{
    	String filePath = "/Users/uuisafresh/Documents/hadoop/Network_inverindex/part-r-00000";
    	readTxtFile(filePath);
    }
     
     
 
}




