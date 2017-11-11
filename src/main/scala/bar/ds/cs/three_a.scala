package bar.ds.cs

import bar.ds.cs.common.{args_parse, read_scala, write_scala, add_idx}

object three_a {

  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string]
    --arr_a_path: full path to array a, csv, no-compression, local
    --arr_b_path: full path to array b, csv, no-compression, local
    --arr_c_path: full path to output array c, csv, no-compression, local
  """

  def main(args: Array[String]): Unit = {

    // Process arguments
    val (path_a, path_b, path_c) = args_parse(args, usage, Array[String]())

    // Read Array A
    val raw_array_a = add_idx(read_scala(path_a))

    // Read Array B
    val raw_array_b = read_scala(path_b)

    // Count number of ele_b that ele_a contains
    def count_b_in_a(raw_array_b: Array[String], ele_a: String): Int = {
      raw_array_b.map{ele_b => if(ele_a.contains(ele_b)) 1 else 0 }.sum
    }

    // Output array
    val array_c = raw_array_a.map{ case (idx, ele_a) => Array(idx.toString, ele_a, count_b_in_a(raw_array_b, ele_a) > 0)}

    // Write output array
    write_scala(path_c, array_c)

  }
}
