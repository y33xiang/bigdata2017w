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

class Conf_6(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_6(argv)

    log.info("Input: " + args.input())
    log.info("Shipdate: " + args.date())
    //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)
    // FileSystem.get(sc.hadoopConfiguration)

    if(args.text()) {
      val lineitem = args.input() + "/lineitem.tbl"
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      if (args.text()) {
        val textFile = sc.textFile(lineitem)
        textFile.filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(inputDate)
        }).map(p => {
          val tokens = p.split("\\|")
          val l_returnflag = tokens(8)
          val l_linestatus = tokens(9)
          val l_quantity = tokens(4).toLong
          val l_extendedprice = tokens(5).toDouble
          val l_discount = tokens(6).toDouble
          val l_tax = tokens(7).toDouble

          val disc_price = l_extendedprice * (1 - l_discount)
          val charge = l_extendedprice * (1 - l_discount) * (1 + l_tax)
          val count = 1
          ((l_returnflag, l_linestatus), (l_quantity, l_extendedprice, disc_price, charge, l_discount, count))
        })
          .reduceByKey((a, b) => {
            (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6)
          })
          .sortByKey()
          .map(p => (p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6))
          .collect()
          .foreach(println)
        // .map(p =>("*",p))
        //  .reduceByKey((a,b) => {a+b})


      }
    }else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate

    //  val lineitem = args.input() + "/lineitem.tbl"
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      val lineitemRDD = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")

      val lineitem = lineitemRDD.rdd

        .filter(line => {
          line(10).toString.contains(inputDate)
        }).map(p => {

        val l_returnflag = p(8)
        val l_linestatus = p(9)
        val l_quantity = p(4).toString.toDouble
        val l_extendedprice = p(5).toString.toDouble
        val l_discount = p(6).toString.toDouble
        val l_tax = p(7).toString.toDouble

        val disc_price = l_extendedprice * (1 - l_discount)
        val charge = l_extendedprice * (1 - l_discount) * (1 + l_tax)
        val count = 1
        ((l_returnflag.toString, l_linestatus.toString), (l_quantity, l_extendedprice, disc_price, charge, l_discount, count))
      })
        .reduceByKey((a, b) => {
          (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6)
        })
        .sortByKey()
        .map(p => (p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6))
        .collect()
        .foreach(println)


    }
    /*else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem")
      val textFile = lineitemDF.rdd
      val count = textFile.filter(line =>{
        line.get(10).equals(inputDate)
      })
        .map(line => ("line", 1))
        .reduceByKey(_ + _)
        .foreach(answer => println("ANSWER=" + answer._2))
    }*/
  }
}


