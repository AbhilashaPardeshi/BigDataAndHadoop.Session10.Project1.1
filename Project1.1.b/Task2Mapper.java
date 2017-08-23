package project11.task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import project11.task2.Task2Driver.MyCounters;

public class Task2Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private Text fbiCode;
	private final static LongWritable ONE = new LongWritable(1); 

	
	@Override
	public void setup(Context context)
	{
		fbiCode = new Text();
	}
	
	@Override
	public void map(LongWritable key,Text crimeRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = crimeRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
		
		if( split.length<15 || split[1].equals("") || split[14].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else if (split[14].equals("32")) 
		{
			context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
			
			String strFbiCode = split[14];
			
			System.out.println("MapperClass : Inserting ["+strFbiCode+" , 1] in the context");
			
			fbiCode.set(strFbiCode);
			context.write(fbiCode, ONE);
		}
	}

	
}
