import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;


public class IntStringPairWritable implements WritableComparable<IntStringPairWritable> {
	public int intVal;
	public String strVal;

	public IntStringPairWritable() {
		
	}

	public int getInt() {
		return this.intVal;
	}

	public void setInt(int ruleId) {
		this.intVal = ruleId;
	}

	public String getString() {
		return this.strVal;
	}

	public void setString(String attrValue) {
		this.strVal = attrValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.setInt(in.readInt());
		this.setString(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.intVal);
		out.writeUTF(this.strVal);
	}

	@Override
	public int compareTo(IntStringPairWritable o) {
		int res = this.intVal - o.intVal;
		if (res == 0) {
			return this.strVal.compareTo(o.strVal);
		} else {
			return res;
		}
	}
	
}
