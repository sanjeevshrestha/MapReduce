package com.sanjeevshrestha;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Hybrid extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	

	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Hybrid(), args);
	    System.exit(res);
	  }
	  
	  

	  public static class HybridMap extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String[] words = value.toString().split(" ");

	            for (String word : words) {
	                if (word.matches("^\\w+$")) {
	                    int count = 0;
	                    for (String term : words) {
	                        if (term.matches("^\\w+$") && !term.equals(word)) {
	                            context.write(new Text(word + "," + term), new Text("1"));
	                            count++;
	                        }
	                    }
	                    context.write(new Text(word + ",*"), new Text(String.valueOf(count)));
	                }
	            }
	        }
	  }
	  
	  
	  public static class HybridReduce extends Reducer<Text, Text, Text, Text> {
	        TreeSet<Pair> priorityQueue = new TreeSet<>();

	        public void reduce(Text key, Iterable<Text> values, Context context)
	                throws IOException, InterruptedException {
	            Map<String, Integer> stripe = new HashMap<>();
	            double totalCount = 0;
	            String keyStr = key.toString();

	            for (Text value : values) {
	                String[] stripes = value.toString().split(",");

	                for (String termCountStr : stripes) {
	                    String[] termCount = termCountStr.split(":");
	                    String term = termCount[0];
	                    int count = Integer.parseInt(termCount[1]);

	                    Integer countSum = stripe.get(term);
	                    stripe.put(term, (countSum == null ? 0 : countSum) + count);

	                    totalCount += count;
	                }
	            }

	            for (Map.Entry<String, Integer> entry : stripe.entrySet()) {
	                priorityQueue.add(new Pair(entry.getValue() / totalCount, keyStr, entry.getKey()));

	                if (priorityQueue.size() > 100) {
	                    priorityQueue.pollFirst();
	                }
	            }
	        }

	        protected void cleanup(Context context)
	                throws IOException,
	                InterruptedException {
	            
	            String prevPairKey = "";
	            String prePairStripe = "";
	            
	            while (!priorityQueue.isEmpty()) {
	                Pair pair = priorityQueue.pollFirst();
	                
	                if (pair.key.compareTo(prevPairKey) != 0)
	                {
	                    if (!prevPairKey.isEmpty())
	                    {
	                        context.write(new Text(prevPairKey), new Text(prePairStripe));
	                    }
	                    
	                    prevPairKey = pair.key;
	                    prePairStripe = "";
	                    
	                }
	                String temp = String.format("(%s, %f), ", pair.value, pair.relativeFrequency);
	                prePairStripe += temp;
	               
	            }
	            context.write(new Text(prevPairKey), new Text(prePairStripe));
	        }

	        class Pair implements Comparable<Pair> {
	            double relativeFrequency;
	            String key;
	            String value;

	            Pair(double relativeFrequency, String key, String value) {
	                this.relativeFrequency = relativeFrequency;
	                this.key = key;
	                this.value = value;
	            }

	            @Override
	            public int compareTo(Pair pair) {
	                int thiskey = Integer.parseInt(this.key);
	                int thisvalue = Integer.parseInt(this.value);
	                
	                int pairkey = Integer.parseInt(pair.key);
	                int pairvalue = Integer.parseInt(pair.value);
	                
	                if (pairkey == thiskey)
	                {
	                    if (thisvalue == pairvalue){
	                        return 0;
	                    }
	                    else if (thisvalue > pairvalue){
	                        return 1;
	                    }
	                    else{
	                        return -1;
	                    }
	                }
	                else
	                {
	                   if (thiskey > pairkey){
	                        return 1;
	                    }
	                    else{
	                        return -1;
	                    }
	                }
	                
	            }
	        }
	    }
	  
	

}
