package topRated;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

public class Driver {
	
	Configuration conf=new Configuration();
	 Job job=new Job(conf, "Youtube top 5");
	 
	 job.setJarByClass(Driver.class);
	 job.setMapperClass(Mymap.class);
	 job.setReducerClass(Myreduce.class);
	 job.setCombinerClass(Mycombine.class);
	 job.setNumReduceTasks(1);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 job.setOutputKeyClass(IntWritable.class);
	 job.setOutputValueClass(Text.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 
	 System.exit(job.waitForCompletion(true)?0:1);
	 

}
