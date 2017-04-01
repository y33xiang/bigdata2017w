package ca.uwaterloo.cs.bigdata2017w.assignment7;


import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class BuildInvertedIndexHBase extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

    public static final String[] FAMILIES = { "c" };
    public static final byte[] CF = FAMILIES[0].getBytes();

    private static final class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
        private static final Text WORD = new Text();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();


        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD.set(e.getLeftElement());
                context.write(WORD, new PairOfInts((int) docno.get(), e.getRightElement()));
            }
        }
    }

    private static final class MyTableReducer extends
            TableReducer<Text, PairOfInts, ImmutableBytesWritable> {
     //  private static final IntWritable DF = new IntWritable();





        @Override
        public void reduce(Text key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {
            Iterator<PairOfInts> iter = values.iterator();
            ArrayList<PairOfInts> postings = new ArrayList<>();


            int docno = 0;
            int tf=0 ;
            Put put = new Put(Bytes.toBytes(key.toString()));

            while (iter.hasNext()) {
                postings.add(iter.next().clone());
            }


            // Sort the postings by docno ascending.
            Collections.sort(postings);

            for(int i=0; i<postings.size();i++){
                PairOfInts doctf = postings.get(i);
                docno = doctf.getLeftElement();
                tf = doctf.getRightElement();
                put.addColumn(CF, Bytes.toBytes(docno), Bytes.toBytes(tf));

            }


            context.write(null, put);

        }
    }

    private BuildInvertedIndexHBase() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;


        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
        public String index;

        @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
        public int numReducers = 1;


    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + BuildInvertedIndexHBase.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - config: " + args.config);
        LOG.info(" - index: " + args.index);
        LOG.info(" - reducers: " + args.numReducers);

        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = connection.getAdmin();






        if (admin.tableExists(TableName.valueOf(args.index))) {
            LOG.info(String.format("Index '%s' exists: dropping table and recreating.", args.index));
            LOG.info(String.format("Disabling table '%s'", args.index));
            admin.disableTable(TableName.valueOf(args.index));
            LOG.info(String.format("Droppping table '%s'", args.index));
            admin.deleteTable(TableName.valueOf(args.index));
        }

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.index));
        for (int i = 0; i < FAMILIES.length; i++) {
            HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
            tableDesc.addFamily(hColumnDesc);
        }
        admin.createTable(tableDesc);
        LOG.info(String.format("Successfully created table '%s'", args.index));

        admin.close();



        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexHBase.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args.input));


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);

        job.setMapperClass(MyMapper.class);
        TableMapReduceUtil.initTableReducerJob(args.index, MyTableReducer.class, job);



        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildInvertedIndexHBase(), args);
    }
}
