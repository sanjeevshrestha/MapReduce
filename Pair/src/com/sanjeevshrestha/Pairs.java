package com.sanjeevshrestha;

import java.io.IOException;
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


public class Pairs extends Configured implements Tool {


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Pairs(), args);
    System.exit(res);
  }


  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "pairs");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }



  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
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

  public static class Combine extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(count)));
        }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
        TreeSet<Pair> priorityQueue = new TreeSet<>();
        double totalCount = 0;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String keyStr = key.toString();
            int count = 0;

            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }

            if (keyStr.matches(".*\\*")) {
                totalCount = count;
            } else {
                String[] pair = keyStr.split(",");
                priorityQueue.add(new Pair(count / totalCount, pair[0], pair[1]));

                if (priorityQueue.size() > 100) {
                    priorityQueue.pollFirst();
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
            while (!priorityQueue.isEmpty()) {
                Pair pair = priorityQueue.pollLast();
                
                String str = String.format("(%s, %s)", pair.key, pair.value);
                String strFreq = String.format("%f", pair.relativeFrequency);
                context.write(new Text(str), new Text(strFreq));
            }
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
