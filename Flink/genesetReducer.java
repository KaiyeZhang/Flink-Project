package ml;

import org.apache.flink.api.common.functions.ReduceFunction;

public class genesetReducer implements ReduceFunction<geneset> {

	private static final long serialVersionUID = 1L;

	/**
	 * Returns a new ItemSet instance with 'frequency' as the sum of
	 * the two input ItemSets
	 *
	 */
	@Override
	public geneset reduce(geneset arg0, geneset arg1) throws Exception {
		geneset item = new geneset(arg0.items);
		item.setCount(arg0.getCount() + arg1.getCount());

		return item;
	}

}
