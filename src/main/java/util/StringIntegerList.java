package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StringIntegerList implements Writable {
	public static class StringInteger implements Writable {
		private String s;
		private int t;
		public static Pattern p = Pattern.compile("(.+),(\\d+)");

		public StringInteger() {
		}

		public StringInteger(String s, int t) {
			this.s = s;
			this.t = t;
		}

		public String getString() {
			return s;
		}

		public int getValue() {
			return t;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			String indexStr = arg0.readUTF();

			Matcher m = p.matcher(indexStr);
			if (m.matches()) {
				this.s = m.group(1);
				this.t = Integer.parseInt(m.group(2));
			}
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			StringBuffer sb = new StringBuffer();
			sb.append(s);
			sb.append(",");
			sb.append(t);
			arg0.writeUTF(sb.toString());
		}

		@Override
		public String toString() {
			return s + "," + t;
		}
	}

	public static class StringIntegerArray implements Writable {
		private String s;
		private ArrayList<Integer> t;
		public static Pattern p = Pattern.compile("(.+)");

		public StringIntegerArray() {
		}

		public StringIntegerArray(String s, ArrayList<Integer> t) {
			this.s = s;
			this.t = t;
		}

		public String getString() {
			return s;
		}

		public ArrayList<Integer> getValue() {
			return t;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			String indexStr = arg0.readUTF();
            System.out.println("readField\t"+indexStr);
            System.out.println("s~~~~~~~~~");
            String[] str=indexStr.split("#");
            this.s=str[0];
            String[] position=str[1].split(",");
            ArrayList<Integer> cur=new ArrayList<Integer>();
            for(String itr:position)
            	cur.add(Integer.parseInt(itr));
            this.t=cur;
//			Matcher m = p.matcher(indexStr);
//			if (m.matches()) {
//				//System.out.println(m.group(1));
//				String[] str=m.group(1).split(",");
//				this.s=str[0];
//				System.out.println(str[0]);
//				ArrayList<Integer> cur=new ArrayList<Integer>();
//				for(int i=1;i<str.length;i++){
//					cur.add(Integer.parseInt(str[i]));
//					System.out.println(str[i]);
//				}
//				this.t=cur;
//				System.out.println(t.size());
//			}
		}
		
		@Override
		public void write(DataOutput arg0) throws IOException {
			StringBuffer sb = new StringBuffer();
			
//			sb.append(s);
//			sb.append(",");
//			sb.append(t);
			
			sb.append(s);
			sb.append("#");
			for(int i=0;i<t.size();i++){
				if(i>0)sb.append(",");
				sb.append(t.get(i));
			}
			
			arg0.writeUTF(sb.toString());
		}

		@Override
		public String toString() {
			String output=s+"#";
			for(int i=0;i<t.size();i++){
				if(i>0)output+=",";
				output+=String.valueOf(t.get(i));
			}
			return output;
		}
	}

	
	
	private List<StringIntegerArray> indices;
	private Map<String, ArrayList<Integer>> indiceMap;
	private Pattern p = Pattern.compile("<([^<>]*)>");

	public StringIntegerList() {
		indices = new Vector<StringIntegerArray>();
	}

	public StringIntegerList(List<StringIntegerArray> indices) {
		this.indices = indices;
	}
	
	public StringIntegerList(Map<String, ArrayList<Integer>> indiceMap) {
		this.indiceMap = indiceMap;
		this.indices = new Vector<StringIntegerArray>();
		for (String index : indiceMap.keySet()) {
			this.indices.add(new StringIntegerArray(index, indiceMap.get(index)));
		}
	}

	public Map<String, ArrayList<Integer>> getMap() {
		if (this.indiceMap == null) {
			indiceMap = new HashMap<String, ArrayList<Integer>>();
			for (StringIntegerArray index : this.indices) {
				indiceMap.put(index.s, index.t);
			}
		}
		return indiceMap;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indicesStr = WritableUtils.readCompressedString(arg0);
		readFromString(indicesStr);
	}

	public void readFromString(String indicesStr) throws IOException {
		List<StringIntegerArray> tempoIndices = new Vector<StringIntegerArray>();
		Matcher m = p.matcher(indicesStr);
		while (m.find()) {
			//System.out.println(m.group(1)+"List_read");
			String[] readline = m.group(1).split("#");
	
			String[] position = readline[1].split(",");
			
			ArrayList<Integer> cur = new ArrayList<Integer>();
			for(int i=0;i<position.length;i++){
				//if(position[i].length()==0)continue;
				cur.add(Integer.parseInt(position[i]));
			}
			StringIntegerArray index = new StringIntegerArray(readline[0],cur);
			tempoIndices.add(index);
		}
		this.indices = tempoIndices;
		
		
	}

	
	public void add(StringIntegerArray siv){
		this.indices.add(siv);
	}
	
	public List<StringIntegerArray> getIndices() {
		return Collections.unmodifiableList(this.indices);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeCompressedString(arg0, this.toString());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		ArrayList<Integer> cur=new ArrayList<Integer>();
		for (int i = 0; i < indices.size(); i++) {
			StringIntegerArray index = indices.get(i);
			if (index.getString().contains("<") || index.getString().contains(">"))
				continue;
			sb.append("<");
			sb.append(index.getString());
			sb.append("#");
			cur=index.getValue();
			for(int j=0;j<cur.size();j++){
				if(j>0)sb.append(",");
				sb.append(String.valueOf(cur.get(j)));
			}
			sb.append(">");
			if (i != indices.size() - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}

}
