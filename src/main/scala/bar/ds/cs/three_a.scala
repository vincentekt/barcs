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

    // Read Array B
    bufferedSource = Source.fromFile(path_b)
    val array_b_lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close()

    val pre_output = tmp_a.map{ x => Array(x._1.toString, x._2, math.min(1, array_b_lines.map{y =>
      if(x._2.contains(y)){1}else{0} }.reduce(_+_)) == 1)}


    // Writing output
    val file = new File(path_c)
    val bw = new BufferedWriter(new FileWriter(file))
    pre_output.foreach{x => bw.write(x.mkString("\t") + "\n")
    }

    bw.close()
  }
}
