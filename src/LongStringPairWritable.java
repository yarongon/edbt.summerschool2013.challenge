import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;


public class LongStringPairWritable implements WritableComparable<LongStringPairWritable> {
	public long longVal;
	public String strVal;

	public LongStringPairWritable() {
		
	}

	public long getLong() {
		return this.longVal;
	}

	public void setLong(long ruleId) {
		this.longVal = ruleId;
	}

	public String getString() {
		return this.strVal;
	}

	public void setString(String attrValue) {
		this.strVal = attrValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.setLong(in.readLong());
		this.setString(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.longVal);
		out.writeUTF(this.strVal);
	}

	@Override
	public int compareTo(LongStringPairWritable arg0) {
		// This is because we don't need any sorting
		return 0;
	}
	
}
