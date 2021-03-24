import org.apache.hadoop.io.*;
import java.io.*;

public class CountPair implements Writable {

    private int count;

    public int getCount() {
        return count;
    }

    public CountPair(){
        
    }
    public CountPair(int count) {
        this.count = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
    }
}
