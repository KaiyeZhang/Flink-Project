package ml;

import org.apache.flink.api.common.functions.Partitioner;

public class selfPartitioner implements Partitioner<Integer> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int partition(Integer arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
