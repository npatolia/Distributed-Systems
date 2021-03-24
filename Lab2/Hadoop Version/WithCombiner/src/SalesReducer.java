import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;


public class SalesReducer
  extends Reducer<Text, CountPair, Text, IntWritable> {
   
   @Override
   public void reduce(Text date, 
	Iterable<CountPair> temperatures, Context context)
        throws IOException, InterruptedException {
        int count = 0;
        for(CountPair el: temperatures){
          count += el.getCount();
        } 
        context.write(date, new IntWritable(count));
    }
}
