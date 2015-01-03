package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import PageRank.Constants.Delimiters;


public class PageRankSortMapper  extends Mapper<LongWritable, Text, PageRankKey, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    	double N = Double.parseDouble(context.getConfiguration().get("N"));
    	
    	String[] title_pgrnkArray = value.toString().split(Delimiters.TAB_DELIMITER);
    	String pageTitle = title_pgrnkArray[0];
    	Double pageRank = Double.parseDouble(title_pgrnkArray[1]);
    	if(pageRank >= 5/N )
    		context.write(new PageRankKey(pageRank, pageTitle), value);
	}
 }
