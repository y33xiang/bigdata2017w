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

    object StripesPMI extends Tokenizer {
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


    val lines = textFile.count()
    val countWord = textFile
    .flatMap(line => tokenize(line).distinct)
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .map(word2 => (word2,lines))
    .map(word3 => (word3._1._1,(word3._1._2,word3._2)))


    val countPair = textFile
    .flatMap(line => {
    val tokens = tokenize(line)
/*if (tokens.length > 1) tokens.flatMap(x =>{
 for(0 until 40){
 tokens.map(y=>{if(x!=y) (x,y) else None})).filter(_!=None) else List()
 }}})*/

    if (tokens.length > 1) tokens.take(40).flatMap(x =>

    tokens.map((x, _))) else List()

    })

    .filter(a =>(a._1!=a._2))
    .map(bigram => (bigram, 1))
    .reduceByKey(_ + _)
    .map(pair => ((pair._1._1),((pair._1._2),pair._2)))
    .join(countWord)
    .map(pair => (pair._2._1._1,((pair._1,pair._2._1._2),pair._2._2)))
    .join(countWord)
    .map(pair=>((pair._2._1._1._1,(((pair._1),(scala.math.log10(((pair._2._1._1._2).toDouble*(pair._2._1._2._2).toDouble)/((pair._2._1._2._1).toDouble*(pair._2._2._1).toDouble)),pair._2._1._2._2))))))
    .groupByKey()
    countPair.saveAsTextFile(args.output())
 //   .map{ something => a._1={a._2._1 = a._2._2} }.saveAsTextFile(args.output())
    }
}

