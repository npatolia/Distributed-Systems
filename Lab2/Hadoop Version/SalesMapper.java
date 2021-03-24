import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SalesMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private static Logger THE_LOGGER = Logger.getLogger(SalesCounterDriver.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        THE_LOGGER.debug("I AM IN LOGGER");
        String valueAsString = value.toString().trim();
        String[] tokens = valueAsString.split(",");
        if (tokens.length < 2) {
            return;
        }
        context.write(new Text(tokens[1].trim()), new IntWritable(1));
        //single write, but we can have multiple writes to context
    }
}
