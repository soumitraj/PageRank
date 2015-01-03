package PageRank;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankKeyComparator extends WritableComparator {

	public PageRankKeyComparator(){
		super(PageRankKey.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		PageRankKey key1 = (PageRankKey) w1;
		PageRankKey key2 = (PageRankKey) w2;
		int compare = key1.compareTo(key2);
		return compare;
	}

}
