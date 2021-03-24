import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTimePair
        implements Writable, WritableComparable<DateTimePair> {

    private final Text date = new Text();
    private final Text time = new Text();


    public DateTimePair() {
    }

    public DateTimePair(String date, String time) {
        this.date.set(date);
        this.time.set(time);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        date.write(out);
        time.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        time.readFields(in);
    }

    @Override
    public int compareTo(DateTimePair pair) {
        if(date.compareTo(pair.getDate())==0){
            return time.compareTo(pair.time);
        }
        return date.compareTo(pair.getDate());
    }


    public Text getDate() {
        return date;
    }

    public Text getTime() {
        return time;
    }
}

