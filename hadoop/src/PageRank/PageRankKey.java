package PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PageRankKey implements WritableComparable {

	private double pageRank;
	private String pageTitle;

	public double getPageRank() {
		return pageRank;
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public PageRankKey() {
	}

	public PageRankKey(double pageRank, String pageTitle) {
		this.pageRank = pageRank;
		this.pageTitle = pageTitle;
	}

	public String toString() {
		return (new StringBuilder()).append(this.pageRank).append(this.pageTitle).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank = Double.parseDouble(WritableUtils.readString(in));
		pageTitle = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, Double.toString(pageRank));
		WritableUtils.writeString(out, pageTitle);
	}

	@Override
	public int compareTo(Object key) {
		//Sort by decreasing order.
		int result = 0;
		if (pageRank > ((PageRankKey) key).getPageRank())
			result = -1;
		else if (pageRank == ((PageRankKey) key).getPageRank())
			result = 0;
		else if (pageRank < ((PageRankKey) key).getPageRank())
			result = 1;
		return result;
	}

}
