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

package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Shipdate: " + args.date())
 //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)
   // FileSystem.get(sc.hadoopConfiguration)



    val inputDate = args.date()
  //  val queryDate = inputDate.split("-")


    if(args.text()) {
      val lineitem = args.input()+"/lineitem.tbl"
      val textFile = sc.textFile(lineitem)
      val count = textFile.filter(line => {
        val tokens = line.split("\\|")
        tokens(10).contains(inputDate)
      })
        .map(p =>p(10))
        val num = count.count()
        println("ANSWER=" + num)

      /*   val count = textFile.map(line =>{

      val lineAttr = line.split("\\|")
      val shipDate = lineAttr(10)
      val tupleDate = shipDate.split("-")
     if(inputDate == shipDate ||
        (queryDate.length == 2 && inputDate == tupleDate(0)+"-"+tupleDate(1)) ||
        (queryDate.length == 1 && inputDate == tupleDate(0))) {
        inputDate
      }

    })
      .map(data => (data,1))
      .reduceByKey(_+_)
      .foreach(answer => println("ANSWER=" + answer._2))*/
    }else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
      val textFile = lineitemDF.rdd
      val count = textFile.filter(line =>{
         line.get(10).equals(inputDate)
      })
        .map(p =>p(10))
        val num = count.count()
        println("ANSWER=" + num)
       /* .map(line => ("line", 1))
        .reduceByKey(_ + _)
        .foreach(answer => println("ANSWER=" + answer._2))*/
    }
  }
}


