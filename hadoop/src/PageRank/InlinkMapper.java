package PageRank;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import PageRank.Constants.Delimiters;
import PageRank.InlinkReducer.PAGES;

public class InlinkMapper extends Mapper<LongWritable, Text, Text, Text> {
	private char LINK_SEPERATOR = '|';

	@Override
	public void map(LongWritable key, Text value1, Context context)
			throws IOException, InterruptedException {

		String xmlString = value1.toString();
		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		String value = "1";
		try {
			Document doc = builder.build(in);
			Element root = doc.getRootElement();

			String pageTitle = root.getChild("title").getTextTrim()
					.replace(" ", "_");

			String pageText = root.getChild("revision").getChild("text")
					.getTextTrim().replace(" ", "_");

			int flag = 0;
			// Extracting wikilink
			String[] outlink = StringUtils.substringsBetween(pageText, "[[",
					"]]");

			int outLinkCounter = 0;
			if (null != outlink) {
				outlink = new HashSet<String>(Arrays.asList(outlink))
						.toArray(new String[0]);
				for (String olink : outlink) {

					if ((olink.indexOf("[") != -1)
							|| (olink.indexOf("]") != -1)
							|| (olink.indexOf("Category:") != -1)
							|| (olink.indexOf("#") != -1)
							|| (olink.indexOf("<") != -1)
							|| (olink.indexOf(">") != -1)
							|| (olink.indexOf("[") != -1)
							|| (olink.indexOf("]") != -1)
							|| (olink.indexOf("|") != -1)
							|| (olink.indexOf("{") != -1)
							|| (olink.indexOf("}") != -1)
							|| (olink.indexOf(":") != -1)
							|| (olink.length() > 255)) {
						// Incorrect Input to mapper, skipping the record
						continue;
					}
					if (olink.indexOf(LINK_SEPERATOR) != -1) {
						value = value
								+ Delimiters.TAB_DELIMITER
								+ olink.substring(0,
										olink.indexOf(LINK_SEPERATOR));
						outLinkCounter++;
					} else {
						value = value + "\t" + olink;
						outLinkCounter++;
					}

					if (!((pageTitle.indexOf("[") != -1)
							|| (pageTitle.indexOf("]") != -1)
							|| (pageTitle.indexOf("Category:") != -1)
							|| (pageTitle.indexOf("#") != -1)
							|| (pageTitle.indexOf("<") != -1)
							|| (pageTitle.indexOf(">") != -1)
							|| (pageTitle.indexOf("[") != -1)
							|| (pageTitle.indexOf("]") != -1)
							|| (pageTitle.indexOf("|") != -1)
							|| (pageTitle.indexOf("{") != -1)
							|| (pageTitle.indexOf("}") != -1)
							|| (pageTitle.indexOf(":") != -1)
							|| (pageTitle.length() > 255)
							|| (pageTitle.indexOf("&") != -1) || (pageTitle
								.indexOf(":") != -1))) {
						context.write(new Text(olink), new Text(
								Delimiters.INLINK_POINTER
										+ Delimiters.TAB_DELIMITER + pageTitle));
						context.write(new Text(pageTitle), new Text(
								Delimiters.OUTLINK_POINTER
										+ Delimiters.TAB_DELIMITER + olink));
					}
				}
			} else {
				if (!((pageTitle.indexOf("[") != -1)
						|| (pageTitle.indexOf("]") != -1)
						|| (pageTitle.indexOf("Category:") != -1)
						|| (pageTitle.indexOf("#") != -1)
						|| (pageTitle.indexOf("<") != -1)
						|| (pageTitle.indexOf(">") != -1)
						|| (pageTitle.indexOf("[") != -1)
						|| (pageTitle.indexOf("]") != -1)
						|| (pageTitle.indexOf("|") != -1)
						|| (pageTitle.indexOf("{") != -1)
						|| (pageTitle.indexOf("}") != -1)
						|| (pageTitle.indexOf(":") != -1)
						|| (pageTitle.length() > 255)
						|| (pageTitle.indexOf("&") != -1) || (pageTitle
							.indexOf(":") != -1)))
					context.write(new Text("===NoOutlink==="), new Text(
							pageTitle));
			}
		} catch (JDOMException ex) {
			Logger.getLogger(InlinkMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (IOException ex) {
			Logger.getLogger(InlinkMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}

}