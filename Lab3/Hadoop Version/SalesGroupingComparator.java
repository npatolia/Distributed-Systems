import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SalesGroupingComparator
        extends WritableComparator {

    protected SalesGroupingComparator() {
        super(DateTimePair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTimePair pair = (DateTimePair) wc1;
        DateTimePair pair2 = (DateTimePair) wc2;
        return pair.getDate().compareTo(pair2.getDate());
    }
}
