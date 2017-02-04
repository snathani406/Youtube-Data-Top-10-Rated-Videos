package topRated;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class Mymap extends Mapper<LongWritable, Text, FloatWritable, Text>{
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	{
		String data= value.toString();
		String dataArray[]=data.split("\t");
		FloatWritable outKey= new FloatWritable();
		try{
		String rate= dataArray[6];
		//String ratedBy= dataArray[7];
		float rating= Float.parseFloat(rate);
		
		outKey.set(rating);
		}catch(Exception e){ }
		context.write(outKey, value);
	}

}
