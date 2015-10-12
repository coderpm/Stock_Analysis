import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class FinalOperation {

	/**
	 * @author pratham
	 *Reference: 
	 *1. http://www.tutorialspoint.com/java/java_treemap_class.htm 
	 *2. http://www.tutorialspoint.com/java/java_hashmap_class.htm
	 *3. http://www.tutorialspoint.com/java/java_arraylist_class.htm
	 *4. http://www.tutorialspoint.com/java/java_collections.htm
	 */
	
	
	public static class Map3 extends Mapper<Object, Text, Text, Text>
	{
		private Text key3 = new Text();
		private Text value3 = new Text();

		public void map(Object key, Text value, Context context)
		{
			
			String line = value.toString();
//			System.out.println("The line is again in Second mapper\n"+line);
//			String [] lineArray = line.split("/");
			key3.set("one");
			value3.set(value);
			
			try{
				
				context.write(key3,value3);
				
			}catch(Exception e)
			{
				e.printStackTrace();
			}
			
					
		}//End of Map Function
			
	}//End of MAP1 Class

	
	public static class Reduce3 extends Reducer<Text, Text, Text, Text>
	{

		private Text key4 = new Text();
		private Text value4 = new Text();

		HashMap<String, Double> finalMap = new HashMap<String, Double>();
		HashMap<String,Double> valueMaps = new HashMap<String, Double>();
		ArrayList<Double> onlyValues = new ArrayList<Double>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		
			String fundName;
			Double volatilityValue=0.0d;
			
			for(Text val:values)
			{
				String eachVal= val.toString();
				String [] valArr=eachVal.split("/");
				fundName = valArr[0];
				volatilityValue=Double.parseDouble(valArr[1].trim());
				
		//		finalMap.put(fundName, volatilityValue);
				valueMaps.put(fundName,volatilityValue);
				onlyValues.add(volatilityValue);
			}
			
			//Sort in ascending order
			Collections.sort(onlyValues);
		
			key4.set("Lowest Fund");
			value4.set("Volatility Value");
			try{
				context.write(key4,value4);
			}catch(Exception e)
			{
				e.printStackTrace();
			}

			int counter=0;
			Set<String> keySet= valueMaps.keySet();
			Double valueCheck=0.0;
			
				for(Double v: onlyValues)
				{
					//Looping for keys in hashmap to correspond to value v
					for(String ky : keySet)
					{
						valueCheck=valueMaps.get(ky);
						if(valueCheck==v){
							//finalMap.put(ky,v);
							key4.set(ky);
							value4.set(v.toString());
							try{
								context.write(key4,value4);
							}catch(Exception e)
							{
								e.printStackTrace();
							}
							break;
						}
					}
					counter++;
					if(counter==10)
						break;
				}
//				System.out.println("\n\n\n\nThe values order is  "+onlyValues);
//			System.out.println("The current hashmap is "+valueMaps);
			
				key4.set("Highest Fund");
				value4.set("Volatility Value");
				try{
					context.write(key4,value4);
				}catch(Exception e)
				{
					e.printStackTrace();
				}

			//Sort in Descending order
			Collections.sort(onlyValues,Collections.reverseOrder());
			
			int count=0;
			Set<String> keySt= valueMaps.keySet();
			Double valueChek=0.0;
			
			for(Double v: onlyValues)
			{
				//Looping for keys in hashmap to correspond to value v
				for(String kys : keySt)
				{
					valueChek=valueMaps.get(kys);
					if(valueChek==v){
						//finalMap.put(kys,valueChek);
						key4.set(kys);
						value4.set(v.toString());
						try{
							context.write(key4,value4);
						}catch(Exception e)
						{
							e.printStackTrace();
						}
						break;
					}
				}
					count++;
					if(count==10)
						break;
				}
//			System.out.println("\n\n\n\nThe values order is  "+onlyValues);
				
//			System.out.println("The final hashmap is "+valueMaps);
			
			
			Iterator<Entry<String, Double>> i = finalMap.entrySet().iterator();
			while(i.hasNext())
			{
				Map.Entry<String, Double> y = i.next();
				key4.set(y.getKey());
				value4.set(y.getValue().toString());
			}
			
			    
			
		}//End of reduce function	

	}//End of Reduce1 class

}
