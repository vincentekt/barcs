package bar.ds.cs

import java.io._

import scala.io.Source

object three_a {
  type OptionMap = Map[Symbol, Any]
  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string]\n
    --arr_a_path: full path to array a, csv, no-compression \n, local
    --arr_b_path: full path to array b, csv, no-compression \n, local
    --arr_c_path: full path to output array c, csv, no-compression \n, local
  """

  def args_parse(args: Array[String]): OptionMap = {
    if (args.length == 0) println(usage)
    val arglist = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map

        case "--arr_a_path" :: value :: tail =>
          println("--arr_a_path " + value + " " + tail)
          nextOption(map ++ Map('path_a -> value.toString), tail)

        case "--arr_b_path" :: value :: tail =>
          println("--arr_b_path " + value + " " + tail)
          nextOption(map ++ Map('path_b -> value.toString), tail)

        case "--arr_c_path" :: value :: tail =>
          println("--arr_c_path " + value + " " + tail)
          nextOption(map ++ Map('path_c -> value.toString), tail)

        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)
    return options
  }

  def main(args: Array[String]): Unit = {

    // Process arguments
    val cl_args = args_parse(args)

    val path_a = cl_args.get('path_a).get.toString
    val path_b = cl_args.get('path_b).get.toString
    val path_c = cl_args.get('path_c).get.toString

    // Read Array A
    var bufferedSource = Source.fromFile(path_a)
    val array_a_lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close()

    val tmp_a = array_a_lines.zipWithIndex.map(_.swap)

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val map_a = tmp_a.flatMap{ x =>
      val ele_list = x._2.split(",")
      val zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      (zip_list ++ (ele_list zip List.fill(ele_list.length)("NA"))).map(k => (k._1 -> k._2) -> x._1.toString)
    }.groupBy(_._1).mapValues(x => x.foldLeft[Array[String]](Array.empty)((s, l) => {s :+ l._2}))

    // Read Array B
    bufferedSource = Source.fromFile(path_b)
    val array_b_lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close()

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
    val len_b = tmp_b.map(x => x._1.toString -> x._2.split(",").length).toMap

    // This is where the main logic occur
    val pre_output = map_a.map{ x =>
      // 1. Combine A and B through Topic, e.g.: ((2,3), idx_A, inx_B)
      (x._1, (x._2, map_b.get(x._1).getOrElse(Array[String]())))
      // 2. Get count of interaction between idx_A and idx_B
    }.filter{y => (y._2._1.toList.length > 0) & (y._2._2.toList.length > 0)}
      .map(x => x._2._1 -> x._2._2).map(x => for(x1 <- x._1; x2 <- x._2) yield (x1, x2) -> 1).flatten.groupBy(_._1)
      .mapValues(_.toList.length).map(x =>
      // 3. If count of interaction is equivalent to length of that B element, then that element of A is true.
      if (len_b.get(x._1._2).get <= (x._2 + 1)){
        x._1._1 -> 1
      }
      else {
        x._1._1 -> 0
      })
//    pre_output.foreach(println)

    // Writing output
    val file = new File(path_c)
    val bw = new BufferedWriter(new FileWriter(file))
    tmp_a.map(x => Array(x._1, x._2, pre_output.get(x._1.toString).getOrElse(0) == 1)).foreach{x =>
      bw.write(x.mkString("\t") + "\n")
    }

    bw.close()
  }
}
