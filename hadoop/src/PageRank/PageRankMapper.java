package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import PageRank.Constants.Delimiters;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String OUTLINK_DELIMITER = "==OUTLINK_DELIMITER==\t";
	private String INLINK_DELIMITER = "\t";
	private Text key_inlink_node = new Text();
	private Text inlink_details = new Text();

	public enum BAD_PAGES {
		COUNT
	};

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		int separatorIn = line.indexOf(Delimiters.TAB_DELIMITER);
		String current_node = line.substring(0, separatorIn);
		String pgrnk_outListString = line.substring(separatorIn, line.length());
		String[] pgrnk_outListArray = pgrnk_outListString
				.split(Delimiters.TAB_DELIMITER);
		if (pgrnk_outListArray != null && pgrnk_outListArray.length > 0) {

			String pgrnk = pgrnk_outListArray[1];
			StringBuilder sb = new StringBuilder();
			inlink_details.set(pgrnk + Delimiters.TAB_DELIMITER + current_node
					+ Delimiters.TAB_DELIMITER
					+ (pgrnk_outListArray.length - 2)
					+ Delimiters.TAB_DELIMITER);
			for (int i = 2; i < pgrnk_outListArray.length; i++) {
				key_inlink_node.set(pgrnk_outListArray[i]);
				context.write(key_inlink_node, inlink_details);
				sb.append(pgrnk_outListArray[i] + INLINK_DELIMITER);
			}
			context.write(new Text(current_node), new Text(OUTLINK_DELIMITER
					+ sb.toString()));
		} else {
			context.getCounter(BAD_PAGES.COUNT).increment(1);
		}
	}
}
