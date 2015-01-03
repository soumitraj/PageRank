package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private Text adjacencyList = new Text();
	private String OUTLINK_DELIMITER = "==OUTLINK_DELIMITER==";
	private String INLINK_DELIMITER = "\t";
	private int INDEX_PR = 0;
	private int INDEX_OUTLINK = 2;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double pgrnk = 0;
		double d = Double.parseDouble(context.getConfiguration().get("d"));
		double N = Double.parseDouble(context.getConfiguration().get("N"));
		String outlinks = "";

		boolean hasOutlink = false;
		boolean hasInLink = false;
		for (Text pg_list : values) {
			String list = pg_list.toString();
			if (list.indexOf(OUTLINK_DELIMITER) == 0) {
				outlinks = list.substring(list.indexOf(OUTLINK_DELIMITER)
						+ OUTLINK_DELIMITER.length(), list.length());
				hasOutlink = true;
			} else {
				String[] array = list.split(INLINK_DELIMITER);
				pgrnk = pgrnk + Double.valueOf(array[INDEX_PR])
						/ Double.valueOf(array[INDEX_OUTLINK]);
				hasInLink = true;

			}
		}

		if (hasOutlink) {
			pgrnk = ((1 - d) / N) + (d * pgrnk);
			adjacencyList.set(String.valueOf(pgrnk) + outlinks);
			context.write(key, adjacencyList);
		}
	}
}
