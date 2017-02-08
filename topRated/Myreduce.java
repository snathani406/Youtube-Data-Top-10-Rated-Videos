package topRated;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class Myreduce extends Reducer<FloatWritable, Text, FloatWritable, Text>{
	
	TreeMap<Float, String> map;
	
	public void setup(Context context)
	{
		map= new TreeMap<Float, String>();
	}
	
	public void reduce(FloatWritable key, Iterable<Text> values, Context context)
	{
		int max=0;
		Text mapValue= new Text();
		
		for(Text value:values)
		{
			String data= value.toString();
			String dataArray[]= data.split("\t");
			try{
			String rated= dataArray[7];
			String id= dataArray[0];
			int ratedBy= Integer.parseInt(rated);
			
			if(max<ratedBy)
			{
				max= ratedBy;
				mapValue.set(id);
			}
			}catch(Exception e){ }
		}
		
		map.put(key, mapValue);
		
		if(map.size()>10)
		{
			map.pollFirstEntry();
		}
		
	}
	
	public void cleanup(Context context)throws IOException, InterruptedException
	{
		for(Map.Entry<Float, String> entry:map.entrySet())
		{
			context.write(entry.getKey(), entry.getValue());
		}
	}

}
