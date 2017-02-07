package topRated;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class Mycombine extends Reducer<FloatWritable, Text, FloatWritable, Text>{
	
TreeMap<Float, String> map;
	
	public void setup(Context context)
	{
		map= new TreeMap<Float, String>();
	}
	
	public void reduce(FloatWritable key, Iterable<Text> values, Context context)
	{
		int max=0;
		String mapValue= new String();
		
		for(Text value:values)
		{
			String data= value.toString();
			String dataArray[]= data.split("\t");
			try{
			String rated= dataArray[7];
			//String id= dataArray[0];
			int ratedBy= Integer.parseInt(rated);
			
			if(max<ratedBy)
			{
				max= ratedBy;
				mapValue= data;
			}
			}catch(Exception e){ }
		}
		
		map.put(key.get(), mapValue);
		
		if(map.size()>10)
		{
			map.pollFirstEntry();
		}
		
	}
	
	public void cleanup(Context context)throws IOException, InterruptedException
	{
		for(Map.Entry<FloatWritable, Text> entry:map.entrySet())
		{
			FloatWritable outKey= new FloatWritable();
			Text outValue= new Text();
			
			outKey.set(entry.getKey());
			outValue.set(entry.getValue());
			
			context.write(outKey, outValue);
		}
	}


}
