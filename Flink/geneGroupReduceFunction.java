package ml;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class geneGroupReduceFunction implements GroupReduceFunction<Tuple2<Integer, ArrayList<Integer>>,Tuple2<Integer, String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Tuple2<Integer, ArrayList<Integer>>> in,
			Collector<Tuple2<Integer, String>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String itemSets = "";
		int count = 0;
		ArrayList<ArrayList<Integer>> itemSetList = new ArrayList<ArrayList<Integer>> ();
		for(Tuple2<Integer, ArrayList<Integer>> tuple : in) {
			count = tuple.f0;
			itemSetList.add(tuple.f1);
		}
		Collections.sort(itemSetList, new OutputComparator());
		for(ArrayList<Integer> itemSet : itemSetList) {
			if(itemSet.size() == 0) { continue; }
			for(int geneId : itemSet) {
				itemSets += geneId + ",";
			}
			itemSets = itemSets.substring(0, itemSets.length()-1) + "\t";
		}
		out.collect(new Tuple2<Integer, String>(count,itemSets));
	}

}
