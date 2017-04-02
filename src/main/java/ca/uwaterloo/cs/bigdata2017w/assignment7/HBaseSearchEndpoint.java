package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.*;

//import org.json.simple.JSONArray;


public class HBaseSearchEndpoint extends BooleanRetrievalHBase {
  //  private Stack<Set<Integer>> stack;

    private Map<Integer,String> searchQuery(String q) throws IOException{
        Map<Integer, String> map = new LinkedHashMap<>();


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
       //     System.out.println(i + "\t" + line);
            //print the docno and sentence retrieved
            map.put(i,line);
        }


        return map;
    }

    private class Handler_1 extends AbstractHandler{

        public void handle (String target,
                            Request baseRequest,
                            HttpServletRequest request,
                            HttpServletResponse response)
                throws IOException, ServletException
        {
          //  response.setContentType("text/html;charset=utf-8");
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
         //   response.getWriter().println("<h1>Hello World</h1>");

            JSONArray list = new JSONArray();


            Map<Integer,String> result = searchQuery(baseRequest.getParameter("query"));
            for (Map.Entry<Integer, String> entry : result.entrySet()){
                JSONObject obj = new JSONObject();
                Integer docid = entry.getKey();
                String text = entry.getValue();
                obj.put("docid", docid);
                obj.put("text", text);
                list.add(obj);
            }


            response.getWriter().println(list);

        }

    }

    private HBaseSearchEndpoint(){}





    protected static final class Args {
        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-port", metaVar = "[term]", required = true, usage = "port")
        int port;
    }


    public int run(String[] argv) throws Exception {
        final Args args = new Args();
       // CmdLineParser parser = new CmdLineParser(args_1, ParserProperties.defaults().withUsageWidth(100));
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        initialize(conf, args.index, args.collection);

        Server server = new Server(args.port);
        server.setHandler(new Handler_1());

        server.start();
        server.join();

        return 1;
    }

/*   public static void main(String[] args) throws Exception
    {
        final Args args_1 = new Args();
        Server server = new Server(Integer.parseInt(args_1.port));
        server.setHandler(new HBaseSearchEndpoint());

        server.start();
        server.join();
    }*/

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseSearchEndpoint(), args);
    }

}





