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

class Conf_2(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
mainOptions = Seq(input, output, reducers)
val input = opt[String](descr = "input path", required = true)
val output = opt[String](descr = "output path", required = true)
val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
val log = Logger.getLogger(getClass().getName())

def main(argv: Array[String]) {
val args = new Conf_2(argv)

log.info("Input: " + args.input())
log.info("Output: " + args.output())
log.info("Number of reducers: " + args.reducers())

val conf = new SparkConf().setAppName("Bigram Count")
val sc = new SparkContext(conf)

val outputDir = new Path(args.output())
FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

val textFile = sc.textFile(args.input())

val countWord = textFile
.flatMap(line => {
val tokens = tokenize(line)
if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
})
.map(word => (word.split(" ")(0), 1))
.reduceByKey(_ + _)


val star = textFile
.flatMap(line => {
val tokens = tokenize(line)
if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
})
.map(word => ((word.split(" ")(0)).toString+",*"))
.map(word =>(word,1))
.reduceByKey(_ + _)
//.map(word =>(word._1.split(",")(0),(word._1.split(",")(1),word._2)))
.map(word =>((word._1.split(",")(0),word._1.split(",")(1)),(word._2).toDouble))


val countPair = textFile
.flatMap(line => {
val tokens = tokenize(line)
if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
})
.map(bigram => (bigram, 1))
.reduceByKey(_ + _)
.map(a => (a._1.split(" ")(0),(a._1.split(" ")(1),a._2)))
//     countPair.leftOuterJoin(countWord)
/*        for(i<- 0 to countPair.count()-1){
 countPair._i.get().
 }*/
val Frequency = countPair.join(countWord)
//   Frequency.map(a => ((a._1,((a._2)._1)._1),((((a._2)._1)._2)/(a._2)._2)))
.map(a => ((a._1,a._2._1._1),((a._2._1._2).toDouble/(a._2._2).toDouble)))
.union(star)

.map(pair => (pair._1+" "+pair._2).toString)
Frequency.saveAsTextFile(args.output())

}
}

