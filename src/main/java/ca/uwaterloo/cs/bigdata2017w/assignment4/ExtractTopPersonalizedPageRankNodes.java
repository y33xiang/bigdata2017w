package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

    private static class MyMapper extends
            Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
        private TopScoredObjects[] queue;
        private static int numSources = 0;

        @Override
        public void setup(Context context) throws IOException {
            String[] stringSources = context.getConfiguration().getStrings("sources");
            numSources = stringSources.length;
            int k = context.getConfiguration().getInt("n", 100);
            queue = new TopScoredObjects[numSources];
            for (int i = 0; i < numSources; i++) {
                queue[i] = new TopScoredObjects<>(k);
            }
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {
            for (int i = 0; i < numSources; i++)
                queue[i].add(node.getNodeId(), node.getPageRank().get(i));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            PairOfInts key = new PairOfInts();
            FloatWritable value = new FloatWritable();

            for (int i = 0; i < numSources; i++) {
                for (PairOfObjectFloat<Integer> pair : queue[i].extractAll()) {
                    key.set(pair.getLeftElement(), i);
                    value.set(pair.getRightElement());
                    context.write(key, value);
                }
            }
        }
    }

    private static class MyReducer extends
            Reducer<PairOfInts, FloatWritable, FloatWritable, IntWritable> {
        private static TopScoredObjects<Integer> queue[];
        private static int numSources = 0;
        private static int[] intSources = null;

        @Override
        public void setup(Context context) throws IOException {
            String[] stringSources = context.getConfiguration().getStrings("sources");
            numSources = stringSources.length;
            int k = context.getConfiguration().getInt("n", 100);
            queue = new TopScoredObjects[numSources];
            for (int i = 0; i < numSources; i++) {
                queue[i] = new TopScoredObjects<>(k);
            }
            intSources = new int[numSources];
            for (int i = 0; i < numSources; i++) {
                intSources[i] = Integer.parseInt(stringSources[i]);
            }
        }

        @Override
        public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
                throws IOException {
            Iterator<FloatWritable> iter = iterable.iterator();
            queue[nid.getRightElement()].add(nid.getLeftElement(), iter.next().get());

            // Shouldn't happen. Throw an exception.
            if (iter.hasNext()) {
                throw new RuntimeException();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            FloatWritable key = new FloatWritable();
            IntWritable value = new IntWritable();

            for (int i = 0; i < numSources; i++) {
                //System.out.println("Source: " + intSources[i]);
                for (PairOfObjectFloat<Integer> pair : queue[i].extractAll()) {
                    // We're outputting a string so we can control the formatting.
                    float pagerank = (float) StrictMath.exp(pair.getRightElement());
                    //System.out.println(String.format("%.5f %d", pagerank, pair.getLeftElement()));
                    key.set(pagerank);
                    value.set(pair.getLeftElement());
                    context.write(key, value);
                }
                //System.out.println();
            }
        }
    }

    public ExtractTopPersonalizedPageRankNodes() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCES = "sources";
    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("sources").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(TOP));
        String sources = cmdline.getOptionValue(SOURCES);

        LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + n);
        LOG.info(" - sources: " + sources);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("n", n);
        conf.setStrings("sources", sources);

        Job job = Job.getInstance(conf);
        job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
        job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(PairOfInts.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // Text instead of FloatWritable so we can control formatting

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        String[] stringSources = sources.split(",");

        Path path = new Path(outputPath + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        int nodeid = 0;
        float pagerank = 0;
        int count = 0;
        String line;
        while((line = br.readLine()) != null) {
            if (count % n == 0) {
                System.out.println();
                System.out.println("Source:\t" + stringSources[count / n]);
            }
            String[] lineContent = line.split("\\t");
            pagerank = Float.parseFloat(lineContent[0]);
            nodeid = Integer.parseInt(lineContent[1]);
            System.out.println(String.format("%.5f %d", pagerank, nodeid));
            count++;
        }

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
        System.exit(res);
    }
}