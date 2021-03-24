import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SalesCounterCombiner
    extends Reducer<Text, CountPair, Text, CountPair> {
   
    @Override
    public void reduce(Text key, Iterable<CountPair> values, Context context)
       throws IOException, InterruptedException {
       int count = 0;
       for (CountPair el : values) {
       	 count+=el.getCount();
       }
       context.write(key, new CountPair(count));
    }   
}

