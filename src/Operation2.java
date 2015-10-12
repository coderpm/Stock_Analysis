import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Operation2 {
	
	/**
	 * @author pratham
	 *Reference: 
	 *1. http://www.tutorialspoint.com/java/java_treemap_class.htm 
	 *2. http://www.tutorialspoint.com/java/java_hashmap_class.htm
	 *3. http://www.tutorialspoint.com/java/java_arraylist_class.htm
	 *4. http://www.tutorialspoint.com/java/java_collections.htm
	 */
	
	public static class Map2 extends Mapper<Object, Text, Text, Text>
	{
		private String fundName;	//Name of the fund
		private String restValues;
		
		private Text key1= new Text();
//		private Text value1= new Text();		
		private Text value1 = new Text();
		public void map(Object key, Text value, Context context)
		{
			 
			String line = value.toString();
//			System.out.println("The line is again in Second mapper\n"+line);
			String [] lineArray = line.split("/");
			
			fundName= lineArray[0];
			key1.set(fundName);
			
			restValues=lineArray[1].trim()+"/"+lineArray[2].trim();
//			System.out.println("The rest values are "+restValues+"\n\n");
			
			value1.set(restValues);
			try{
				context.write(key1, value1);
			}catch(Exception e)
			{
				e.printStackTrace();
			}
				}//End of Map Function
			
	}//End of MAP1 Class

	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text>
	{
		private Text key2= new Text();
		private Text value2 = new Text();

		TreeMap<String,Double> mapValues = new TreeMap<String, Double>();
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//Set the key as fund name
			String keywrite=key.toString()+"/";
			
			String month;
			Double monthRet;
			int monthCount=0;
			Double monthReturnTotal=0.0;
			Double monthReturnAverage=0.0;
			Double volatility;
			Double squareValueTotal=0.0;
			Double tempValue=0.0;
			
			//Write the fund name as the key2
			key2.set(keywrite);
			
			for(Text val:values)
			{
				String eachVal= val.toString();
				String [] valArr=eachVal.split("/");
				month = valArr[0];
				monthRet = Double.parseDouble(valArr[1]);
//				System.out.println("THE MONTH RETURN IS"+monthRet+"\n\n\n****\n\n");
				mapValues.put(month, monthRet);

			}
			for(String iter: mapValues.keySet()){
				monthCount++;
				monthReturnTotal=monthReturnTotal+mapValues.get(iter);
			}
			monthReturnAverage=monthReturnTotal/monthCount;
			
//			System.out.println("The sum values are "+monthReturnTotal);
//			System.out.println("The month count are "+monthCount);
//			System.out.println("The month Average values are "+monthReturnAverage+" And squareValuetotal is:"+squareValueTotal);
			
			for(String iter:mapValues.keySet())
			{
//				System.out.println("The iter value is "+iter+" and value is "+mapValues.get(iter));
				tempValue=mapValues.get(iter)-monthReturnAverage;
//				tempValue=tempValue*tempValue;
				tempValue=Math.pow(tempValue, 2);
				squareValueTotal=squareValueTotal+tempValue;
			}
//			System.out.println("The month Average values are "+monthReturnAverage+" And squareValuetotal is:"+squareValueTotal);

			if(!(monthCount==1))
			{
				try{
					Double lastValue=0.0d;
					monthCount=monthCount-1;
					lastValue= squareValueTotal/monthCount;
//					System.out.println("The last value afteR dividing is"+lastValue);
					volatility = Math.sqrt(lastValue);
//					System.out.println("The VOLATILITY is"+volatility);

					String vola = volatility.toString();
//					System.out.println("MonthCount is "+monthCount);
//					System.out.println("LastValue is "+lastValue);
					
					value2.set(vola);
				if(!(volatility==0.0))	
				{
					context.write(key2, value2);

				}
				}catch(Exception e)
				{
					e.printStackTrace();
				}
	
			}//End of IF
			else
			{
				System.out.println("No uSe printing");
			}
			
			//Resetting all the value
			month=null;
			monthRet=0.0;
			monthCount=0;
			monthReturnTotal=0.0;
			monthReturnAverage=0.0;
			volatility=0.0;
			squareValueTotal=0.0;
			tempValue=0.0;
			
			mapValues.clear();

				
			
		}//End of reduce function	
		
	}//End of Reduce1 class


}
