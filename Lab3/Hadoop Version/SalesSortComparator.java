import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The DateTemperatureGroupingComparator class
 * enable us to compare two DateTemperaturePair 
 * objects. This class is needed for sorting 
 * purposes.
 *
 * @author Mahmoud Parsian
 *
 */
public class SalesSortComparator
        extends WritableComparator {

    protected SalesSortComparator() {
        super(DateTimePair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTimePair pair = (DateTimePair) wc1;
        DateTimePair pair2 = (DateTimePair) wc2;
        return pair.compareTo(pair2);
    }
}