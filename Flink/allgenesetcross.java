package ml;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.CrossFunction;

public class allgenesetcross implements CrossFunction<geneset, geneset, geneset> {

	private static final long serialVersionUID = 1L;

	@Override
	public geneset cross(geneset arg0, geneset arg1) throws Exception {
		// create a new ArrayList of items
		ArrayList<Integer> items = arg0.items;

		// only add new items
		for (Integer item : arg1.items) {
			if (!items.contains(item)) {
				items.add(item);
			}
		}

		// create a new ItemSet
		geneset newItemSet = new geneset(items);
		// set a temporary number of transactions
		newItemSet.setCount(arg0.getCount());

		return newItemSet;
	}

}
