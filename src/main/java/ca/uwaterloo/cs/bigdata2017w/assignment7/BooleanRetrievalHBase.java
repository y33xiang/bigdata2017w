package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.NavigableSet;
import java.util.NavigableMap;
import java.util.Iterator;

public class BooleanRetrievalHBase extends Configured implements Tool {
    public Stack<Set<Integer>> stack;
    Table tableIndex;
    Table tableCollection;

    //private BooleanRetrievalHBase() {}
    protected BooleanRetrievalHBase() {}
    protected void initialize(Configuration conf, String index, String collection) throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        tableIndex = connection.getTable(TableName.valueOf(index));
        tableCollection = connection.getTable(TableName.valueOf(collection));

        stack = new Stack<>();
    }

    protected void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
                //word,docno,docno,docno......
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
            //print the docno and sentence retrieved
        }
    }

    protected void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    protected void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    protected void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    protected Set<Integer> fetchDocumentSet(String term) throws IOException {
        //second fetch: get list of docno
        Set<Integer> set = new TreeSet<>();
        NavigableSet<byte[]> docnoes = fetchPostings(term);
        Iterator<byte[]> docnoIterator = docnoes.iterator();

        while (docnoIterator.hasNext()) {
            byte[] docno = docnoIterator.next();
            set.add(Bytes.toInt(docno));
        }

        return set;
    }

    protected NavigableSet<byte[]> fetchPostings(String term) throws IOException {
        //first fetch: get list of (docno,tf)
        Get get = new Get(Bytes.toBytes(term));
        Result result = tableIndex.get(get);
        NavigableMap<byte[],byte[]> map = result.getFamilyMap(BuildInvertedIndexHBase.CF);
        NavigableSet<byte[]> set = map.navigableKeySet();

        return set;
    }

    protected String fetchLine(long offset) throws IOException {
        //fetch from the collection(document),then give the sentence
        Get get = new Get(Bytes.toBytes(Long.toString(offset)));
        Result result = tableCollection.get(get);
        String d = Bytes.toString(result.getValue(InsertCollectionHBase.CF, InsertCollectionHBase.CONTENT));

        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    protected static final class Args {
        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        String query;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        initialize(conf, args.index, args.collection);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalHBase(), args);
    }
}