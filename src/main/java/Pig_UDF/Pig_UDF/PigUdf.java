package Pig_UDF.Pig_UDF;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class PigUdf extends FilterFunc {
	
	public Boolean exec(Tuple input) throws IOException {
		try {
			
			Object BPLobj = input.get(0);
			Object BPLper = input.get(1);
			
			double result = 0.8 * (Integer.parseInt(BPLobj.toString()));
			
			return (Integer.parseInt(BPLper.toString())) >= result;
		} catch (Exception e) {
			
			throw new IOException("Something bad happened!" , e);
		}
	}

}
