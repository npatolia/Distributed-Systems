import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class SalesReducer
        extends Reducer<DateTimePair, Text, Text, Text> {

    @Override
    protected void reduce(DateTimePair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String result="";
        for (Text value : values) {
            result += (value.toString()+",");
        }
        result = result.substring(0, result.length()-1);
        context.write(key.getDate(), new Text(result));
    }
}
