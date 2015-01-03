package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import PageRank.Constants.Delimiters;

public class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {
		String line = value1.toString().trim();
		if(line.split(Delimiters.TAB_DELIMITER).length >0 ){
		String pageTitleKey = line.split(Delimiters.TAB_DELIMITER)[0];

		double N = Double.parseDouble(context.getConfiguration().get("N"));
		double pageRank = 1/N;
		
		String outlinks = line.substring(line.indexOf(Delimiters.TAB_DELIMITER)+1, line.length()); 
		
		context.write(new Text(pageTitleKey), new Text(pageRank + Delimiters.TAB_DELIMITER + outlinks) );
		}
	}

}
