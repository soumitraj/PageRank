package PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import PageRank.Constants.Delimiters;

public class InlinkReducer extends Reducer<Text, Text, Text, Text> {
	HashMap<String, Integer> pageCount = new HashMap<String, Integer>();

	public enum PAGES {
		COUNT
	};

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<String> inlinkList = new ArrayList<String>();
		ArrayList<String> outlinkList = new ArrayList<String>();

		StringBuilder inlinkString = new StringBuilder();
		StringBuilder outlinkString = new StringBuilder();

		for (Text value : values) {
			if (value.toString().split(Delimiters.TAB_DELIMITER).length >= 2) {
				if (value.toString().split(Delimiters.TAB_DELIMITER)[0]
						.equalsIgnoreCase(Delimiters.INLINK_POINTER)) {
					inlinkList.add(value.toString().split("\t")[1]);
					inlinkString.append(value.toString().split(
							Delimiters.TAB_DELIMITER)[1]
							+ Delimiters.TAB_DELIMITER);
				}
				if (value.toString().split(Delimiters.TAB_DELIMITER)[0]
						.equalsIgnoreCase(Delimiters.OUTLINK_POINTER)) {
					outlinkList.add(value.toString().split(
							Delimiters.TAB_DELIMITER)[1]);
					outlinkString.append(value.toString().split(
							Delimiters.TAB_DELIMITER)[1]
							+ Delimiters.TAB_DELIMITER);
				}
				if (key.toString().equalsIgnoreCase(
						Delimiters.NO_OUTLINK_POINTER)) {
					context.write(value, new Text(""));
				}
			}
		}

		// / Nodes which have outlinks and which have inlinks --> These nodes
		// contribute to page rank and N
		if (inlinkList.size() > 0 && outlinkList.size() > 0) {
			context.write(key, new Text(outlinkString.toString().trim()));
			context.getCounter(PAGES.COUNT).increment(1);
		}
		if (outlinkList.size() > 0 && inlinkList.size() == 0) {
			context.write(key, new Text(outlinkString.toString()));
			context.getCounter(PAGES.COUNT).increment(1);
		}
	}
}
