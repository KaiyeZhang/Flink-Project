package ml;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class gptransactions implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, ArrayList<Integer>>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Tuple2<Integer, Integer>> in, Collector<Tuple2<Integer, ArrayList<Integer>>> out)
			throws Exception {
		ArrayList<Integer> genes = new ArrayList<>();
		int patientId = -1;
		for (Tuple2<Integer, Integer> transaction : in) {
			genes.add(transaction.f1);
			patientId = transaction.f0;
		}
		if(patientId >= 0) {
			out.collect(new Tuple2<Integer, ArrayList<Integer>>(patientId, genes));
		}
	}

}
