package ml;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class genesetoccurences extends RichMapFunction<geneset, geneset> {

	private static final long serialVersionUID = 1L;
	private Collection<Tuple2<Integer, ArrayList<Integer>>> transactions;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.transactions = getRuntimeContext().getBroadcastVariable("transactions");
	}

	@Override
	public geneset map(geneset arg0) throws Exception {

		geneset out = new geneset(arg0.items);
		int count = 0;

		for (Tuple2<Integer, ArrayList<Integer>> transaction : this.transactions) {
			if (transaction.f1.containsAll(arg0.items)) {
				count++;
			}
		}

		out.setCount(count);
		return out;
	}

}
