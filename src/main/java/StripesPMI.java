/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.MapKF;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.pair.PairOfStringFloat;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;




public class StripesPMI  extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);
    
    private static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text WORD = new Text();
        private static final IntWritable ONE = new IntWritable(1);
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
            List<String> tokens =Tokenizer.tokenize(value.toString());
            ArrayList<String> appeared = new ArrayList<String>();
            
            
            if(tokens.size()>40){
                tokens = tokens.subList(0,39);
            }
            
            for(int i=0;i<tokens.size();i++){
                String cur = tokens.get(i);
                if(!appeared.contains(cur)){
                    appeared.add(cur);
                }
            }
            
            for(int i=0;i<appeared.size();i++){
                String word = appeared.get(i);
                WORD.set(word);
                context.write(WORD,ONE);
            }
            
            WORD.set("*");
            context.write(WORD,ONE);
            
        }
    }
    
    
    private static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable SUM = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while(iter.hasNext()){
                sum+=iter.next().get();
            }
            SUM.set(sum);
            context.write(key,SUM);
        }
    }
    
    
    private static final class MyMapper2 extends Mapper<LongWritable, Text, Text, PairOfStringFloat> {
     //   private static final IntWritable ONE = new IntWritable(1);
        private static final float ONE = 1.0f;
        private static final PairOfStringFloat WORD = new PairOfStringFloat();
        private static final Text LEFT = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
            List<String> tokens = Tokenizer.tokenize(value.toString());
            ArrayList<String> appeared = new ArrayList<String>();

            if(tokens.size()>40){
                tokens = tokens.subList(0,39);
            }
            
            for(int i=0;i<tokens.size();i++){
                String cur = tokens.get(i);
                if(!appeared.contains(cur)){
                    appeared.add(cur);
                }
            }
            
            
            for(int i=0;i<appeared.size()-1;i++){
                for(int j=i+1;j<appeared.size();j++){
                    LEFT.set(appeared.get(j));
                    WORD.set(appeared.get(i),ONE);
                    context.write(LEFT,WORD);
                    
                    LEFT.set(appeared.get(i));
                    WORD.set(appeared.get(j),ONE);
                    context.write(LEFT,WORD);
                    
                }
            }

            
            /*  for(int i=0 ; i < appeared.size()-1;i++){
                HashMapWritable<Text, IntWritable> map = new HashMapWritable();
                for (int j =i+1 ; j<appeared.size();j++){
                    if(appeared.get(i).equals(appeared.get(j)))continue;
                    if(map.get(new Text(appeared.get(j))) == null){
                        map.put(new Text(appeared.get(j)), ONE);
                    }
                    else{
                        IntWritable val = (IntWritable) map.get(new Text(appeared.get(j)));
                        IntWritable temp = new IntWritable( val.get() + 1 );
                   //     val+=1;
                        map.put(new Text(appeared.get(j)),val);
                    }
                }
                if (!map.isEmpty()) {
                    context.write(new Text(appeared.get(i)), map);
                }
            }*/
            
            
        }
    }
    
    private static final class MyCombiner extends Reducer<Text, PairOfStringFloat, Text, PairOfStringFloat> {
        
        private static final PairOfStringFloat SUM = new PairOfStringFloat();
        @Override
   /*     public void reduce(Text key, Iterable<PairOfStringInt> values, Context context)
        throws IOException, InterruptedException {
            IntWritable value = new IntWritable();
            int sum=0;
            Text word = new Text();
         //   HashMapWritable<Text, IntWritable> tmp = new HashMapWritable();
            PairOfStringInt tmp = new PairOfStringInt();
  //          Iterator<HashMapWritable> iter = values.iterator();
            Iterator<PairOfStringInt> iter = values.iterator();
            while(iter.hasNext()){
                tmp = iter.next();
                word = tmp.getLeftElement();
          //      value = iter.get(word);
         //       sum = sum +(Integer)value;
            }
            SUM.put(word,sum);
            SUM.set(word);
            context.write(key,SUM);
        }*/
        
        public void reduce(Text key, Iterable<PairOfStringFloat> values, Context context)
        throws IOException, InterruptedException{
            int sum = 0;
            String word=" ";
            Iterator<PairOfStringFloat> iter = values.iterator();
            PairOfStringFloat tmp;
            while(iter.hasNext()){
                tmp = iter.next();
                sum+= tmp.getRightElement();
                word = tmp.getLeftElement();
            }
            
            SUM.set(word,sum);
            context.write(key,SUM);
            
        }
        
    }
    
    private static final class MyReducer2 extends Reducer<Text, PairOfStringFloat, Text, HashMapWritable> {
        
            private static final HashMapWritable<Text,PairOfFloatInt> VALUE = new HashMapWritable();
        private static HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
        
        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            
            String intermediatePath = conf.get("intermediatePath");
            Path inFile = new Path(intermediatePath + "/part-r-00000");
            
            if(!fs.exists(inFile)){
                throw new IOException("File Not Found: "+inFile.toString());
            }
            
            BufferedReader reader = null;
            try{
                FSDataInputStream in = fs.open(inFile);
                InputStreamReader inStream = new InputStreamReader(in);
                reader = new BufferedReader(inStream);
            }catch(FileNotFoundException e){
                throw new IOException("Failed to open file ");
            }
            
            LOG.info("Start reading file from "+ intermediatePath);
            String line = reader.readLine();
            while(line != null){
                String[] word = line.split("\\s+");
                if(word.length !=2){
                    LOG.info("Input line is not valid: '"+line+"'");
                }else{
                    wordMap.put(word[0], Integer.parseInt(word[1]));
                }
                line = reader.readLine();
            }
            
            LOG.info("Finish reading file.");
            reader.close();
        }
        
        @Override
        public void reduce(Text key, Iterable<PairOfStringFloat> values, Context context)
        throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            float numThreshold = Float.parseFloat(conf.get("threshold"));
            float sum = 0.0f;
            String word = " ";
            Iterator<PairOfStringFloat> iter = values.iterator();
              PairOfStringFloat tmp = new PairOfStringFloat();
              Text right = new Text();
            
            while(iter.hasNext()){
                tmp = iter.next();
                sum+= tmp.getRightElement();
                word = tmp.getLeftElement();

            }
            
            if(sum>=numThreshold){
                PairOfFloatInt pair = new PairOfFloatInt();
           //     PairOfStrings tupel = new PairOfStrings();
                String leftWord = key.toString();
                String rightWord = word;
                Integer totalVal = wordMap.get("*");
                Integer leftVal = wordMap.get(leftWord);
                Integer rightVal = wordMap.get(rightWord);
                
                if(totalVal !=null && leftVal !=null && rightVal !=null){
                    float pmiVal = (float)Math.log10(1.0f*sum*totalVal/(leftVal * rightVal));
                    right.set(rightWord);
                    pair.set(pmiVal,(int)sum);
                      VALUE.put(right,pair);
                    context.write(key,VALUE);
                    
                }
            }
        }
    }
    
    
    private StripesPMI() {}
    
    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;
        
        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;
        
        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
        
        @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
        boolean textOutput = true;
        
        @Option(name = "-threshold", metaVar = "[num]", usage = "threashold of co-occurence pairs")
        int numThreshold = 10;
        
    }
    
    
    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));//200
        
        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }
        
        String intermediatePath = args.output + "-tmp";
        
        LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + intermediatePath);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);
        LOG.info(" - num threshold: " + args.numThreshold);
        
        Configuration conf =getConf();
        conf.set("threshold", Integer.toString(args.numThreshold));
        conf.set("intermediatePath", intermediatePath);
        
        Job job = Job.getInstance(getConf());
        job.setJobName(StripesPMI.class.getSimpleName());
        job.setJarByClass(StripesPMI.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(intermediatePath));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        
        Path intermediateDir = new Path(intermediatePath);
        FileSystem.get(getConf()).delete(intermediateDir, true);
        
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("The first job finished.");
        
        
        //second job
        LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - text output: " + args.textOutput);
        LOG.info(" - num threshold: " + args.numThreshold);
        
        Job job2 = Job.getInstance(getConf());
        job2.setJobName(StripesPMI.class.getSimpleName());
        job2.setJarByClass(StripesPMI.class);
        
        job2.setNumReduceTasks(args.numReducers);
        
        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(PairOfStringFloat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(HashMapWritable.class);
        
        if (args.textOutput) {
            job2.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        }
        
        job2.setMapperClass(MyMapper2.class);
        job2.setCombinerClass(MyCombiner.class);
        job2.setReducerClass(MyReducer2.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);
        
        job2.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        
        return 0;
    }
    
    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }
}








