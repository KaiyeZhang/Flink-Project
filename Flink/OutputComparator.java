package ml;

import java.util.ArrayList;
import java.util.Comparator;

public class OutputComparator implements Comparator<ArrayList<Integer>> {

	@Override
	public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
		// TODO Auto-generated method stub
		if(o1.size() > o2.size()) {
			return 1;
		}
		if(o1.size() < o2.size()) {
			return -1;
		}
		return 0;
	}  
}
