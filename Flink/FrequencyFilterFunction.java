package ml;

import org.apache.flink.api.common.functions.FilterFunction;

public class FrequencyFilterFunction implements FilterFunction<geneset> {

	private static final long serialVersionUID = 1L;

	private final long minCount;

	public FrequencyFilterFunction(long minCount) {
		this.minCount = minCount;
	}

	/**
	 * Filters ItemSets with frequency < minCount
	 *
	 */
	@Override
	public boolean filter(geneset arg0) throws Exception {
		return arg0.getCount() >= this.minCount;
	}

}
