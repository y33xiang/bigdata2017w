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



class Conf_3(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input,output,model,method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model", required = true)
  val method = opt[String](descr = "method", required = true)

  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    val group_xrdd = sc.textFile(args.model()+"/part-00000")
    val group_yrdd = sc.textFile(args.model()+"/part-00001")
    val britneyrdd = sc.textFile(args.model()+"/part-00002")
    val textFile = sc.textFile(args.input())

    val group_x = group_xrdd.map(line =>{
      val len = line.length
      val newline = line.substring(1,len-1)
      val tokens = newline.split(",")
      val m_features = tokens(0).toInt
      val m_spamness = tokens(1).toDouble
      (m_features,m_spamness)
    })
      .collectAsMap()
    val group_xHashmap = sc.broadcast(group_x)



    val group_y = group_yrdd.map(line =>{
      val len = line.length
      val newline = line.substring(1,len-1)
      val tokens = newline.split(",")
      val m_features = tokens(0).toInt
      val m_spamness = tokens(1).toDouble
      (m_features,m_spamness)
    })
      .collectAsMap()
    val group_yHashmap = sc.broadcast(group_y)




    val britney = britneyrdd.map(line =>{
      val len = line.length
      val newline = line.substring(1,len-1)
      val tokens = newline.split(",")
      val m_features = tokens(0).toInt
      val m_spamness = tokens(1).toDouble
      (m_features,m_spamness)
    })
      .collectAsMap()
    val britneyHashmap = sc.broadcast(britney)


    if(args.method() == "average"){
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
          var score_x = 0d
          var score_y = 0d
          var score_b = 0d
          var predection = "ham"

          features.foreach(f =>{
            if(group_xHashmap.value.contains(f))
              score_x += group_xHashmap.value(f)

            if(group_yHashmap.value.contains(f))
              score_y += group_yHashmap.value(f)

            if(britneyHashmap.value.contains(f))
              score_b += britneyHashmap.value(f)

          })
          score = (score_x+score_y+score_b)/3
          if (score >0) predection = "spam"

          (docid,isSpam,score,predection)
        }).saveAsTextFile(args.output())


    }else if (args.method() == "vote"){
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
          var score_x = 0d
          var score_y = 0d
          var score_b = 0d
          var predection = "ham"

          features.foreach(f =>{
            if(group_xHashmap.value.contains(f))
              score_x += group_xHashmap.value(f)

            if(group_yHashmap.value.contains(f))
              score_y += group_yHashmap.value(f)

            if(britneyHashmap.value.contains(f))
              score_b += britneyHashmap.value(f)

          })
          if(score_x >0) score_x = 1 else score_x = -1
          if(score_y >0) score_y = 1 else score_y = -1
          if(score_b >0) score_b = 1 else score_b = -1

          score = score_x+score_y+score_b
          if(score>0) predection = "spam"
          (docid,isSpam,score,predection)
        }).saveAsTextFile(args.output())

        }



  }
}


