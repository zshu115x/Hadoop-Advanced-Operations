package Project3.Query1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GridKey implements WritableComparable<GridKey>{
	private Text Gridkey = new Text();
	private IntWritable Flag = new IntWritable();
	public static final IntWritable RECT_RECORD = new IntWritable(0);
	public static final IntWritable POINT_RECORD = new IntWritable(1);
	public Text getGridKey(){
		return Gridkey;
	}
	public IntWritable getFlag(){
		return Flag;
	}
	public GridKey(){}
	public GridKey(String key, int order){
		Gridkey.set(key);
		Flag.set(order);
	}
	@Override
	public int compareTo(GridKey gkey) {
		int compareVal = this.Gridkey.compareTo(gkey.getGridKey());
		if (compareVal == 0) compareVal = this.Flag.compareTo(gkey.getFlag());
		return compareVal;
	}
	@Override
	public void readFields(DataInput In) throws IOException {
		Gridkey.readFields(In);
		Flag.readFields(In);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Gridkey.write(out);
		Flag.write(out);
	}
	public boolean equals (GridKey other) {
		return this.Gridkey.equals(other.getGridKey()) && this.Flag.equals(other.getFlag() );
	}
	public int hashCode() {
		return this.Gridkey.hashCode();
	}
}
