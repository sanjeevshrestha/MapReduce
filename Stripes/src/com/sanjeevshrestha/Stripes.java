package com.sanjeevshrestha;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Stripes extends Configured implements Tool {


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Stripes(), args);
		System.exit(res);

	}

	@Override
	  public int run(String[] args) throws Exception{
		
		Job job = Job.getInstance(getConf(), "Stripes");
	    job.setJarByClass(this.getClass());
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(StripesMap.class);
	    job.setReducerClass(StripesReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class StripesMap extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String word : words) {
				if (word.matches("^\\w+$")) {
					Map<String, Integer> stripe = new HashMap<>();

					for (String term : words) {
						if (term.matches("^\\w+$") && !term.equals(word)) {
							Integer count = stripe.get(term);
							stripe.put(term, (count == null ? 0 : count) + 1);
						}

					}

					StringBuilder stripeStr = new StringBuilder();
					for (Iterator<Entry<String, Integer>> iterator = stripe.entrySet()
							.iterator(); iterator.hasNext();) {
						Entry<String, Integer> entry = iterator.next();
						stripeStr.append(entry.getKey()).append(":")
								.append(entry.getValue()).append(",");
					}

					if (!stripe.isEmpty()) {
						context.write(new Text(word),
								new Text(stripeStr.toString()));
					}

				}
			}
		}
	}
	
	
	 public static class StripesCombine extends Reducer<Text, Text, Text, Text> {
		    public void reduce(Text key, Iterable<Text> values, Context context)
		                throws IOException, InterruptedException {
		            Map<String, Integer> stripe = new HashMap<>();

		            for (Text value : values) {
		                String[] stripes = value.toString().split(",");

		                for (String termCountStr : stripes) {
		                    String[] termCount = termCountStr.split(":");
		                    String term = termCount[0];
		                    int count = Integer.parseInt(termCount[1]);

		                    Integer countSum = stripe.get(term);
		                    stripe.put(term, (countSum == null ? 0 : countSum) + count);
		                }
		            }

		            StringBuilder stripeStr = new StringBuilder();
		            for (Iterator<Entry<String, Integer>> iterator = stripe.entrySet()
							.iterator(); iterator.hasNext();) {
						Entry<String, Integer> entry = iterator.next();
						stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
					}

		            context.write(key, new Text(stripeStr.toString()));
		        }
		  }
	 
	 
	 public static class StripesReduce extends Reducer<Text, Text, Text, Text> {
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
