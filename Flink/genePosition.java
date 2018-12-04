package ml;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.flink.api.java.functions.KeySelector;

public class genePosition implements KeySelector<geneset, String> {

	private static final long serialVersionUID = 1L;

	/**
	 * Group by the concatenated, sorted Integer values
	 * in the list of values in the ArrayList<> of ItemSet
	 *
	 */
	@Override
	public String getKey(geneset arg0) throws Exception {
		String key = null;
		ArrayList<Integer> items = arg0.items;

		Collections.sort(items);

		for (Integer item : items) {
			key += item.toString();
		}
		return key;
	}


}
