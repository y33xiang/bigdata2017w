package ca.uwaterloo.cs.bigdata2017w.assignment7;

        import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;

public class InsertCollectionHBase extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(InsertCollectionHBase.class);

    public static final String[] FAMILIES = { "p" };
    public static final byte[] CF = FAMILIES[0].getBytes();
    public static final byte[] CONTENT = "content".getBytes();

    private static final class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {

            //identity mapper
            context.write(docno, doc);
        }
    }

    private static final class MyTableReducer extends
            TableReducer<LongWritable, Text, ImmutableBytesWritable> {

        public void reduce(LongWritable docno, Iterable<Text> doc, Context context)
                throws IOException, InterruptedException {

            Put put = new Put(Bytes.toBytes(docno.toString()));

            for (Text value : doc) put.addColumn(CF, CONTENT, Bytes.toBytes(value.toString()));

            context.write(null, put);
        }
    }

    private InsertCollectionHBase() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
        public String index;

        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

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

        LOG.info("Tool: " + InsertCollectionHBase.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output table: " + args.index);
        LOG.info(" - config: " + args.config);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(args.index))) {
            LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.index));
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

        Job job = Job.getInstance(conf);
        job.setJobName(InsertCollectionHBase.class.getSimpleName());
        job.setJarByClass(InsertCollectionHBase.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        TableMapReduceUtil.initTableReducerJob(args.index, MyTableReducer.class, job);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InsertCollectionHBase(), args);
    }
}