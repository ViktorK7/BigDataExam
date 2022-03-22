package Exam.BigDataExam;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
	
	
	public static String firstDate2;
	public   static String lastDate2;
	public  static String country;
	public static String city;
	public static boolean exSearch;
	public static String product;
	public static String result;
	
    
    @Override
	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
    	
    	String line = value.toString();

        String[] array = line.split(",");
    
        
  
        if(exSearch) {
            if(country.length() != 0 && array[7].equalsIgnoreCase(country))
            {
                if(city.length() != 0) 
                {
                    if(array[5].equalsIgnoreCase(city)) 
                    {
                        output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
                    }
                }
                else 
                {
                    output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
                }
            }
            else if(city.length() != 0 && array[5].equalsIgnoreCase(city))
            {
                output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
            }

        } else 
        {
            if(country.length() != 0)
            {
                if(array[7].toUpperCase().contains(country.toUpperCase())) 
                {
                    if(city.length() != 0)
                    {
                        if(array[5].toUpperCase().contains(city.toUpperCase())) 
                        {
                            output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
                        }
                    } 
                    else
                    {
                        output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
                    }
                }
            } 
            else if(city.length() != 0 && array[5].toUpperCase().contains(city.toUpperCase()))
            {
                output.collect(new Text(array[7]), new FloatWritable(Float.parseFloat(array[2])));
            }
        }
		
		
		
	}

}
