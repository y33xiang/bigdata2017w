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

package ca.uwaterloo.cs.bigdata2017w.assignment6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._
import org.apache.hadoop.fs._



class Conf_2(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input,output,model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model", required = true)

  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)



    val train = sc.textFile(args.model())
    val textFile = sc.textFile(args.input())

    val trained = train.map(line =>{
      val len = line.length
      val newline = line.substring(1,len-1)
      val tokens = newline.split(",")
      val m_features = tokens(0).toInt
      val m_spamness = tokens(1).toDouble
      (m_features,m_spamness)
    })
      .collectAsMap()
    val trainedHashmap = sc.broadcast(trained)

    val test = textFile.map(line =>{
      val tokens = line.split(" ")
      val docid = tokens(0)
      val isSpam = tokens(1)
      val features = tokens.drop(2).map(_.toInt)
      (docid,isSpam,features)
    })
      .map(line =>{
        val docid = line._1
        val isSpam = line._2
        val features = line._3
        var score = 0d
        var predection = "ham"
        features.foreach(f =>{
          if(trainedHashmap.value.contains(f))
            score += trainedHashmap.value(f)
        })
        if (score >0) predection = "spam"

        (docid,isSpam,score,predection)

      })



    test.saveAsTextFile(args.output())


  }
}


