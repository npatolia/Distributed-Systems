import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopNMapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {
    public static final int DEFAULT_N = 10;
    private int n = DEFAULT_N;

    private TreeSet<Record> top = new TreeSet<Record>();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] tokens = line.split(",");

        double price = Double.parseDouble(tokens[2].trim());
        top.add(new Record(Integer.parseInt(tokens[0].trim()),tokens[1].trim(),price));

        if (top.size() > n) {
            top.remove(top.last());
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        this.n = context.getConfiguration().getInt("N", DEFAULT_N); // default is top 10
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Record r : top) {
            context.write(NullWritable.get(), new Text(r.toString()));
        }
    }
}
