import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {
    public static class InputScannerMapper extends Mapper < Object, Text, LongWritable, LongWritable > {
       public void map(Object key, Text value, Context context)
       throws IOException,
       InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long key2 = s.nextLong();
            long value2 = s.nextLong();
	    context.write(new LongWritable(key2), new LongWritable(value2));
	    s.close();
	}
    }

    public static class CountNumberOfNeighboursReducer extends Reducer < LongWritable, LongWritable, Text, Text > {
         public void reduce(LongWritable key, Iterable < LongWritable > nodes, Context context)
         throws IOException,
         InterruptedException {
              long count = 0;
              for (LongWritable node: nodes) {
                  count++;
               };
               context.write(new Text(String.valueOf(key)), new Text(String.valueOf(count)));
               }
    }
    	
    public static class GroupNeighbourCountMapper extends Mapper < Object, Text, IntWritable, Text > {
         public void map(Object key, Text value, Context context)
         throws IOException,
         InterruptedException {
              String [] keyValue = value.toString().split("\t");
	      String count = keyValue[0];
	      context.write(new IntWritable(Integer.parseInt(count)), new Text(Integer.toString(1)));
	}
   }
   public static class NodeGroupReducer extends Reducer < IntWritable, Text, IntWritable, Text > {
        public void reduce(IntWritable key, Iterable < Text > values, Context context)
        throws IOException,
        InterruptedException {
            int sum = 0;
	    for (Text value: values) {
	        sum = sum + Integer.parseInt(value.toString());
	    };
	    context.write(key,new Text(Integer.toString(sum)));
	}
    }


    public static void main ( String[] args ) throws Exception {
	    Job job1 = Job.getInstance();
	    String output = "temp";
	    job1.setJobName("MyJob");
	    job1.setJarByClass(Graph.class);
	    job1.setMapperClass(InputScannerMapper.class);
	    job1.setReducerClass(CountNumberOfNeighboursReducer.class);
	    job1.setMapOutputKeyClass(LongWritable.class);
	    job1.setMapOutputValueClass(LongWritable.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(output));
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job1.waitForCompletion(true);

	    Job job2 = Job.getInstance();
	    job2.setJobName("MyJob1");
	    job2.setJarByClass(Graph.class);
	    job2.setMapperClass(GroupNeighbourCountMapper.class);
	    job2.setReducerClass(NodeGroupReducer.class);
	    job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(output));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
   }
}
