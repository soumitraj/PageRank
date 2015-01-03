package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import PageRank.Constants.Delimiters;

public class PageRankSortReducer  extends Reducer<PageRankKey, Text, Text, Text>{
    public void reduce(PageRankKey key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
    	
    	context.write(new Text(key.getPageTitle()), new Text(Double.toString(key.getPageRank())));
     
    }
 }
