package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

	public static class StringIntegerVector implements Writable {
		private String s;
		private Vector<Integer> t;
		public static Pattern p = Pattern.compile("<([^<>]*)>");

		public StringIntegerVector() {
		}

		public StringIntegerVector(String s, Vector<Integer> t) {
			this.s = s;
			this.t = t;
		}

		public String getString() {
			return s;
		}

		public Vector<Integer> getValue() {
			return t;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			String indexStr = arg0.readUTF();

			Matcher m = p.matcher(indexStr);
			if (m.matches()) {
				//System.out.println(m.group(1));
				String[] str=m.group(1).split(",");
				this.s=str[0];
				Vector<Integer> cur=new Vector<Integer>();
				for(int i=1;i<str.length;i++)
					cur.add(Integer.parseInt(str[i]));
				this.t=cur;
				System.out.println(t.size());
			}
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			StringBuffer sb = new StringBuffer();
			sb.append(this.toString());
//			sb.append(s);
//			sb.append(",");
//			sb.append(t);
			
			sb.append(s);
			for(int i=0;i<t.size();i++){
				sb.append(",");
				sb.append(t.get(i));
			}
			
			arg0.writeUTF(sb.toString());
		}

		@Override
		public String toString() {
			String output=s;
			for(int i=0;i<t.size();i++)
				output+=","+String.valueOf(t.get(i));
			return output;
		}
	}

	
	
	private List<StringIntegerVector> indices;
	private Map<String, Vector<Integer>> indiceMap;
	private Pattern p = Pattern.compile("<([^<>]*)>");

	public StringIntegerList() {
		indices = new Vector<StringIntegerVector>();
	}

	public StringIntegerList(List<StringIntegerVector> indices) {
		this.indices = indices;
	}
	
	public StringIntegerList(Map<String, Vector<Integer>> indiceMap) {
		this.indiceMap = indiceMap;
		this.indices = new Vector<StringIntegerVector>();
		for (String index : indiceMap.keySet()) {
			this.indices.add(new StringIntegerVector(index, indiceMap.get(index)));
		}
	}

	public Map<String, Vector<Integer>> getMap() {
		if (this.indiceMap == null) {
			indiceMap = new HashMap<String, Vector<Integer>>();
			for (StringIntegerVector index : this.indices) {
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
		List<StringIntegerVector> tempoIndices = new Vector<StringIntegerVector>();
		Matcher m = p.matcher(indicesStr);
		while (m.find()) {
			String[] readline = m.group(1).split(",");
			Vector<Integer> cur = new Vector<Integer>();
			for(int i=1;i<readline.length;i++)
				cur.add(Integer.parseInt(readline[i]));
			StringIntegerVector index = new StringIntegerVector(readline[0],cur);
			tempoIndices.add(index);
		}
		this.indices = tempoIndices;
		
		
	}

	
	public void add(StringIntegerVector siv){
		this.indices.add(siv);
	}
	
	public List<StringIntegerVector> getIndices() {
		return Collections.unmodifiableList(this.indices);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeCompressedString(arg0, this.toString());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		Vector<Integer> cur=new Vector<Integer>();
		for (int i = 0; i < indices.size(); i++) {
			StringIntegerVector index = indices.get(i);
			if (index.getString().contains("<") || index.getString().contains(">"))
				continue;
			sb.append("<");
			sb.append(index.getString());
			cur=index.getValue();
			for(int j=0;j<cur.size();j++){
			    sb.append(",");
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
