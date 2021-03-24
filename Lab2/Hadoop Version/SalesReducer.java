import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;


public class SalesReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text date,
                       Iterable<IntWritable> sales, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable el: sales){
            count += 1;
        }
        context.write(date, new IntWritable(count));
    }
}
