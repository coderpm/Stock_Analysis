import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Operation1 {

	/**
	 * @author pratham
	 *Reference: 
	 *1. http://www.tutorialspoint.com/java/java_treemap_class.htm 
	 *2. http://www.tutorialspoint.com/java/java_hashmap_class.htm
	 *3. http://www.tutorialspoint.com/java/java_arraylist_class.htm
	 *4. http://www.tutorialspoint.com/java/java_collections.htm
	 */
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
	{
		private String fundName;
		private String newDate;
		private String newAdjustment;

		private String writeDate;
		private String writeAdjustment;

		private Text key1 = new Text();//set as the column of A or row of B.
		private Text value1 = new Text();//set as the column of A or row of B.

		public void map(LongWritable key, Text value, Context context)
		{
			
			//Getting the file name
			Path filePath= ((FileSplit)context.getInputSplit()).getPath();
			String tempFundName = filePath.getName();
			String [] names= tempFundName.split(".csv");
			fundName=names[0];
			//System.out.println("The fund name is :::"+fundName);
			
			//String [] dateSplit = names[1].split("-");
			

			//getting the line of the csv file
			String line = value.toString();
			if(line!=null)
			{
	//			System.out.println("The line is : "+line );
				String [] lineValues = line.split(",");
				newDate=lineValues[0];
				newAdjustment=lineValues[6];
				
				try{
				if(!(newDate.equalsIgnoreCase("Date")))
				{
					
					writeDate=fundName+"/";
					String [] dateArray= newDate.split("-");
					
				//	writeDate=writeDate.concat(dateArray[0]);
				//	writeDate=writeDate.concat(dateArray[1]);
					
					writeDate =writeDate + dateArray[0] +dateArray[1]+"/";
					
					writeAdjustment = dateArray[2]+"-";

					//writeAdjustment=writeAdjustment.concat(newAdjustment);

					writeAdjustment=writeAdjustment+newAdjustment;
			
					key1.set(writeDate);
					value1.set(writeAdjustment);
					
					context.write(key1, value1);

				}//End of If checking Date word
				
				}catch(Exception e)
				{
					e.printStackTrace();
				}
			}				
				
				
				
			
		}//End of Map Function
			
	}//End of MAP1 Class

	
	public static class Reduce1 extends Reducer<Text, Text, Text, Text>
	{

		private Text key2 = new Text();
		private Text value2 = new Text(); 
//		private DoubleWritable value2 = new DoubleWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
//				ArrayList<Text> lists = new ArrayList<Text>();
			TreeMap<Integer,String> valueMap= new TreeMap<Integer, String>();
//		
			key2.set(key.toString());
			
			String item;
	
			for (Text value: values){
				String eachValue = value.toString();
//				System.out.println("The values are "+eachValue+"/n/n/n");
				String [] eachValueArray = eachValue.split("-");
				int month = Integer.parseInt(eachValueArray[0]);
				valueMap.put(month, eachValueArray[1]);
			}
			try{
				
				//Set the first and last of the treemap to get first and last of the month
				Entry<Integer, String> first = valueMap.firstEntry();
				Entry<Integer, String> last =  valueMap.lastEntry();
				
//				int firstdate = first.getKey();
				String firstValue= first.getValue();
//				item = Integer.toString(firstdate)+"-"+firstValue;
						
				String lastValue= last.getValue();
//				item = Integer.toString(lastdate)+"-"+lastValue;
				
				Double monthReturn = Double.parseDouble(lastValue)- Double.parseDouble(firstValue);
				monthReturn = monthReturn/Double.parseDouble(firstValue);
				item = monthReturn.toString();
				
				value2.set(item);
				context.write(key2,value2);
				
			}catch(Exception e)
			{
				e.printStackTrace();
			}
			valueMap.clear();
		}//End of reduce function	

	}//End of Reduce1 class
	
}//End of Operation1 class
