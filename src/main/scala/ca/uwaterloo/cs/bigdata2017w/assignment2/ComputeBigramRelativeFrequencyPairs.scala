package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
    mainOptions = Seq(input, output, reducers)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    verify()
    }

    object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val countWord = textFile
   /* .flatMap(line => {

    val tokens = tokenize(line)
 //  if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
    if (tokens.length > 1) tokens.map(p => p.mkString(" ")).toList else List()
    })*/
    .flatMap(line => tokenize(line))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

    val countPair = textFile
        .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
        })
        .map(bigram => (bigram, 1))
        .reduceByKey(_ + _)
        .map(a => (a._1.split(" ")(0),(a._1.split(" ")(1),a._2)))
	countPair.join(countWord)
	
	 val Frequency = countPair.join(countWord)
//       	 .map(a => ((a._1,a._2._1._1),(a._2._1._2/a._2._2)))
	.map(a => ((a._1,a._2._1._1),((a._2._1._2).toDouble/(a._2._2).toDouble)))  
  Frequency.saveAsTextFile(args.output())

    }
}
