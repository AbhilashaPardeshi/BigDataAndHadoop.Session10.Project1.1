package project11.task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import project11.task3.Task3Driver.MyCounters;


public class Task3Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private Text district;
	private final static LongWritable ONE = new LongWritable(1); 

	
	@Override
	public void setup(Context context)
	{
		district = new Text();
	}
	
	@Override
	public void map(LongWritable key,Text crimeRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = crimeRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
		
		if( split.length<12 || split[5].equals("") || split[8].equals("")  || split[11].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else if (split[5].trim().equalsIgnoreCase("THEFT") && split[8].trim().equalsIgnoreCase("TRUE")) 
		{
			context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
			
			String strDistrict = split[11];
			
			System.out.println("MapperClass : Inserting ["+strDistrict+" , 1] in the context");
			
			district.set(strDistrict);
			context.write(district, ONE);
		}
	}

	
}
