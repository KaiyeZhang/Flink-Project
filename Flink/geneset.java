package ml;

import java.util.ArrayList;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


public class geneset {

	public ArrayList<Integer> items;
	private int count;

	// empty geneSet
	public geneset() {
		this.items = new ArrayList<>();
		this.count = 0;
	}

	// geneSet from an item
	public geneset(Integer item) {
		this.items = new ArrayList<>();
		this.items.add(item);
		this.count = 1;
	}

	// geneSet from list of items
	public geneset(ArrayList<Integer> itemList) {
		this.items = itemList;
	}
	
	public ArrayList<Integer> getItems() {
		return items;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getCount() {
		return count;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		if (obj.getClass() != getClass()) {
			return false;
		}

		geneset rhs = (geneset) obj;
		return new EqualsBuilder()
				.appendSuper(super.equals(obj))
				.append(items, rhs.items)
				.append(count, rhs.count)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31)
				.append(items)
				.append(count)
				.toHashCode();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int geneId : items) {
			sb.append(geneId+" ");
		}
		return sb.toString();
	}
}
