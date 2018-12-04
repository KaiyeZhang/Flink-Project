package ml;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class OutputMapFunction implements MapFunction<geneset, Tuple2<Integer, ArrayList<Integer>>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Each item of a transaction looks like (tid, item) in the data
	 *
	 * (1, 10) | (2, 10) | (3, 10) | (4, 10)
	 * (1, 20) | (2, 20) | (3, 20) |
	 * (1, 30) | (2, 30) |         |
	 * (1, 40) |
	 *
	 * This function maps each 'item' to the value 1, producing
	 * for example (10, 1), (20, 1), (30, 1), (40, 1), (10, 1) ...
	 *
	 * The (10, 1) is an ItemSet instance:
	 * - ArrayList<Integer> 'items' holding the 10
	 *
	 */
	@Override
	public Tuple2<Integer, ArrayList<Integer>> map(geneset item) throws Exception {
		return new Tuple2<Integer, ArrayList<Integer>> (item.getCount(), item.getItems());
	}

}
