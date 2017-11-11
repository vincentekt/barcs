package bar.ds.cs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.io.Source
import bar.ds.cs.common.{args_parse, read_scala, write_spark}

object three_b {

  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n
    --arr_b_path: full path to array b, csv, no-compression \n, local
    --arr_c_path: full path to output array c, csv, no-compression \n
    --fs: hadoop or local \n
  """

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    // Process arguments
    val (path_a, path_b, path_c) = args_parse(args, usage, Array[String]("path_a", "path_c"))

    // Read Array A
    val array_a = sc.textFile(path_a)

    val tmp_a = array_a.zipWithIndex().map(_.swap)
    tmp_a.cache()

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val map_a = tmp_a.flatMap{ x =>
      val ele_list = x._2.split(",")
      val zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      (zip_list ++ (ele_list zip List.fill(ele_list.length)("NA"))).map(k => (k._1 -> k._2) -> x._1.toString)
    }.reduceByKey(_+","+_).mapValues(_.split(","))

    // Read Array B
    val array_b_lines = read_scala(path_b)

    val tmp_b = array_b_lines.zipWithIndex.map(_.swap)

    // Extract ("Topic", index) for Array B, e.g.: ((2,3), 0)
    val map_b = tmp_b.flatMap{ x =>
      val ele_list = x._2.split(",")
      var zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      if (ele_list.length == 1){
        zip_list = ele_list zip Array("NA")
      }

      zip_list.map(k => (k._1 -> k._2) -> x._1.toString)
    }.groupBy(_._1).mapValues(x => x.foldLeft[Array[String]](Array.empty)((s, l) => {s :+ l._2}))

    // Extract lengths of Array B elements
    val len_b = tmp_b.map(x => x._1.toString -> x._2.split(",").length)
    val len_b_bc = sc.broadcast(len_b)
    val map_b_bc = sc.broadcast(map_b.toArray)

    // This is where the main logic occur
    val pre_output = map_a.mapPartitions { x =>
      val map_b_val = map_b_bc.value.toMap
      x.map{y =>
        // 1. Combine A and B through Topic, e.g.: ((2,3), idx_A, inx_B)
        (y._1, (y._2, map_b_val.get(y._1).getOrElse(Array[String]())))
      }.filter{y => (y._2._1.toList.length > 0) & (y._2._2.toList.length > 0)}}
      // 2. Explodes (Flatten) them prior to counting. e.g.: ((2,3), (idx_A_1, idx_A_2), (idx_B_1, idx_B_2)) -->
      // (idx_A_1, idx_B_1), (idx_A_1, idx_B_2), (idx_A_2, idx_B_2), (idx_A_2, idx_B_1)
      .map(x => x._2._1 -> x._2._2).flatMapValues(x=>x).map(_.swap).flatMapValues(x=>x)
      // 3. Get count of interaction between idx_A and idx_B
      .map(x => (x._2, x._1) -> 1).reduceByKey(_+_).mapPartitions { x =>
      val len_b_val = len_b_bc.value.toMap

      x.map{x =>
        // 4. If count of interaction is equivalent to length of that B element, then that element of A is true.
        if(len_b_val.get(x._1._2).get <= (x._2 + 1)){
          x._1._1 -> 1
        } else {
          x._1._1 -> 0
        }
      }
      // 5. For every element of A, find if such element exists in B
    }.reduceByKey(_+_).mapValues(math.min(_, 1))

    val array_c = tmp_a.map(x => x._1.toString -> x._2).leftOuterJoin(pre_output)
      .map(x => (x._1, x._2._1, x._2._2.getOrElse(0) == 1) )

    // Writing output
    write_spark(path_c, sqlContext, array_c)
  }
}
