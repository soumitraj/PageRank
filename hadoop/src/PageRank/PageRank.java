package PageRank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import PageRank.Constants.InputOutputPaths;

public class PageRank {
	public static String bucketName = "";

	public static void main(String args[]) {
		try {
			bucketName = args[0];
			runJobs();
		} catch (IOException ex) {
			Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null,
					ex);
		}
	}

	public static void runJobs() throws IOException {

		int total_Pages = buildInlinGraph();

		buildInlinGraphWithPageRank(total_Pages);

		saveNToFileSystem(total_Pages);

		calculatePageRank(total_Pages);
		
		sortByRank(total_Pages);

		saveDataToTarget();
	}

	private static int buildInlinGraph() throws IOException {
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "Build_InLink_Graph");

		FileInputFormat.setInputPaths(job, InputOutputPaths.INPUT_FILE_PATH);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(InlinkMapper.class);
		job.setReducerClass(InlinkReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String output = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_0_JOB1;
		Path outPath = new Path(output);

		FileOutputFormat.setOutputPath(job, outPath);

		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(new Path(output))) {
			dfs.delete(new Path(output), true);
		}

		try {

			job.waitForCompletion(true);

		} catch (InterruptedException ex) {
			Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null,
					ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null,
					ex);
		}
		return Integer.parseInt(Long.toString((job.getCounters().findCounter(
				InlinkReducer.PAGES.COUNT).getValue())));
	}

	private static void buildInlinGraphWithPageRank(int total_Pages)
			throws IOException {

		Configuration config;
		Job jobconf;
		Path inpath, outpath;
		config = new Configuration();
		config.set("N", "" + total_Pages);
		jobconf = new Job(config);
		jobconf.setJobName("PageRank_Itr_0Job2"); // Different iterations have
													// different job names as
													// per iteration number
		jobconf.setMapperClass(PageRankJob2Mapper.class);
		jobconf.setJarByClass(PageRank.class);

		String input = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_0_JOB1;
		inpath = new Path(input);
		String output = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_0_JOB2;
		outpath = new Path(output);
		FileInputFormat.addInputPath(jobconf, inpath);
		FileOutputFormat.setOutputPath(jobconf, outpath);

		jobconf.setInputFormatClass(TextInputFormat.class);
		jobconf.setOutputFormatClass(TextOutputFormat.class);
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);
		try {
			jobconf.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void saveNToFileSystem(int N) {
		try {
			String output = InputOutputPaths.S3N + bucketName
					+ InputOutputPaths.TEMP_DIRECTORY + InputOutputPaths.N_PATH;
			Path path = new Path(output);
			FileSystem fileSystem = FileSystem.get(path.toUri(),
					new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fileSystem.create(path, true)));
			String out = "N=" + N;
			br.write(out);
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void calculatePageRank(int total_Pages) throws IOException {
		Configuration config;
		Job job;
		int itrnum = 0; // to keep track of number of recursion calls
		Path inpath, outpath;
		// Setting the number of iterations parameter from input
		int ITERATIONS = 8;
		int bad_pages = 0;
		itrnum++;
		while (itrnum <= ITERATIONS) {
			// Using the configuration reference from a completeley new object
			config = new Configuration();
			config.set("d", Constants.D_RESIDUAL_PROBABILITY); // Updating the
																// damping
			// factor from the input
			config.set("N", "" + total_Pages);

			job = new Job(config);
			job.setJobName("PageRank_Itr_" + itrnum); // Different
														// iterations have
														// different job
														// names as per
														// iteration number
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);

			job.setJarByClass(PageRank.class);
			// setting for first iteration

			if (itrnum == 1)
				inpath = new Path(InputOutputPaths.S3N + bucketName
						+ InputOutputPaths.TEMP_DIRECTORY
						+ InputOutputPaths.ITER_0_JOB2);
			else
				inpath = new Path(InputOutputPaths.S3N + bucketName
						+ InputOutputPaths.TEMP_DIRECTORY
						+ InputOutputPaths.ITR_NUM + (itrnum - 1) + "/");
			outpath = new Path(InputOutputPaths.S3N + bucketName
					+ InputOutputPaths.TEMP_DIRECTORY
					+ InputOutputPaths.ITR_NUM + itrnum);

			// setting the map-reduce job for execution
			FileInputFormat.addInputPath(job, inpath);
			FileOutputFormat.setOutputPath(job, outpath);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Wait for current iteration and then update the iteration counter
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			itrnum++;
			bad_pages = bad_pages
					+ Integer.parseInt(Long.toString((job.getCounters()
							.findCounter(PageRankMapper.BAD_PAGES.COUNT)
							.getValue())));
		}
	}

	private static void sortByRank(int total_Pages) throws IOException {
		Path inpath, outpath;

		Configuration config = new Configuration();
		config.set("N", "" + total_Pages);

		Job jobconf = new Job(config);
		jobconf.setJobName("Sort_Itrnum_1");

		jobconf.setJarByClass(PageRank.class);

		jobconf.setInputFormatClass(TextInputFormat.class);
		jobconf.setOutputFormatClass(TextOutputFormat.class);
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);

		jobconf.setMapperClass(PageRankSortMapper.class);
		jobconf.setMapOutputKeyClass(PageRankKey.class);
		jobconf.setSortComparatorClass(PageRankKeyComparator.class);
		jobconf.setReducerClass(PageRankSortReducer.class);
		jobconf.setNumReduceTasks(1);

		String input = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY + InputOutputPaths.ITR_NUM
				+ "1" + "/";
		inpath = new Path(input);
		String output = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_1_SORTED_PATH;
		outpath = new Path(output);
		FileInputFormat.addInputPath(jobconf, inpath);
		FileOutputFormat.setOutputPath(jobconf, outpath);

		try {
			jobconf.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		config = new Configuration();
		config.set("N", "" + total_Pages);

		jobconf = new Job(config);
		jobconf.setJobName("Sort_Itrnum_8");

		jobconf.setJarByClass(PageRank.class);

		jobconf.setInputFormatClass(TextInputFormat.class);
		jobconf.setOutputFormatClass(TextOutputFormat.class);
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);

		jobconf.setMapperClass(PageRankSortMapper.class);
		jobconf.setMapOutputKeyClass(PageRankKey.class);
		jobconf.setSortComparatorClass(PageRankKeyComparator.class);
		jobconf.setReducerClass(PageRankSortReducer.class);
		jobconf.setNumReduceTasks(1);

		input = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY + InputOutputPaths.ITR_NUM
				+ "8" + "/";
		inpath = new Path(input);
		output = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_8_SORTED_PATH;
		outpath = new Path(output);
		FileInputFormat.addInputPath(jobconf, inpath);
		FileOutputFormat.setOutputPath(jobconf, outpath);

		try {
			jobconf.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void saveDataToTarget() throws IOException {
		// Copy inlink graph
		String inputDir = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_0_JOB1;
		String outputFilePath = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.RESULTS_DIRECTORY
				+ InputOutputPaths.INLINK_GRAPH;
		copySrcToDsc(inputDir, outputFilePath);

		// Copy N
		inputDir = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY + InputOutputPaths.N_PATH;
		outputFilePath = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.RESULTS_DIRECTORY + InputOutputPaths.N_PATH;
		copySrcToDsc(inputDir, outputFilePath);

		// Copy Iteration 1 result
		inputDir = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_1_SORTED_PATH;
		outputFilePath = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.RESULTS_DIRECTORY + InputOutputPaths.ITER_1;
		copySrcToDsc(inputDir, outputFilePath);

		// Copy Iteration 8 result
		inputDir = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.TEMP_DIRECTORY
				+ InputOutputPaths.ITER_8_SORTED_PATH;
		outputFilePath = InputOutputPaths.S3N + bucketName
				+ InputOutputPaths.RESULTS_DIRECTORY + InputOutputPaths.ITER_8;
		copySrcToDsc(inputDir, outputFilePath);
	}

	public static void copySrcToDsc(String inputDir, String outputFilePath)
			throws IOException {
		try {
			Path outPath = new Path(outputFilePath);
			FileSystem fs = FileSystem
					.get(outPath.toUri(), new Configuration());
			FileStatus[] status = fs.listStatus(new Path(inputDir));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					fs.create(outPath, true)));
			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(status[i].getPath())));
				String line;
				line = br.readLine();
				while (line != null) {
					bw.write(line);
					bw.newLine();
					line = br.readLine();
				}
				br.close();
			}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
