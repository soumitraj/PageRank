package PageRank;

public class Constants {

	public static final String D_RESIDUAL_PROBABILITY = "0.85";

	class Delimiters {

		public static final String TAB_DELIMITER = "\t";

		public static final String INLINK_POINTER = "===Inlink===";

		public static final String OUTLINK_POINTER = "===Outlink===";

		public static final String NO_OUTLINK_POINTER = "===NoOutlink===";

		public static final String OUTLINK_DELIMITER = "\t\t";

		public static final String INLINK_DELIMITER = "\t";
	}

	class InputOutputPaths {

		public static final String INPUT_FILE_PATH = "s3n://spring-2014-ds/data/enwiki-latest-pages-articles.xml";

		public static final String S3N = "s3n://";

		public String bucketName = "";

		public static final String RESULTS_DIRECTORY = "/results/";

		public static final String TEMP_DIRECTORY = "/tmp/";

		public static final String N_PATH = "PageRank.n.out";

		public static final String ITER_0_JOB1 = "iter_0Job1";

		public static final String ITER_0_JOB2 = "iter_0Job2";

		public static final String INLINK_GRAPH = "PageRank.outlink.out";

		public static final String ITR_NUM = "itrnum_";

		public static final String ITER_1 = "PageRank.iter1.out";

		public static final String ITER_8 = "PageRank.iter8.out";

		public static final String ITER_1_SORTED_PATH = "itrnum1_Sorted";

		public static final String ITER_8_SORTED_PATH = "itrnum8_Sorted";
	}
}
