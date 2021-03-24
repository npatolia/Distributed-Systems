import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;

public class SalesMapper extends
          Mapper<LongWritable, Text, Text, CountPair> {
   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(",");
      if (tokens.length < 2) {
         return;
      }
      context.write(new Text(tokens[1].trim()), new CountPair(1));
     //single write, but we can have multiple writes to context
   }
}

