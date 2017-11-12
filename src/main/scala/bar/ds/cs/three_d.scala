package bar.ds.cs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import bar.ds.cs.common._

object three_d {

  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n
    --arr_b_path: full path to array b, csv, no-compression \n
    --arr_c_path: full path to output array c, csv, no-compression \n
    --fs: hadoop or local \n
  """

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    // Process arguments
    val (path_a, path_b, path_c) = args_parse(args, usage, Array[String]("path_a", "path_c"))

    //------------------------------------------------------------------------------------------------
    // i) Preprocess array A
    //------------------------------------------------------------------------------------------------
    // Read Array A
    val ary_A = add_idx(sc.textFile(path_a)).cache()

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val a_memberships = get_global_topic_membership(get_local_topic_membership(ary_A, "A"))

    //------------------------------------------------------------------------------------------------
    // ii) Preprocess array B
    //------------------------------------------------------------------------------------------------
    // Read Array B
    val ary_B = add_idx(sc.textFile(path_b)).cache()

    // Extract lengths of Array B elements and broadcast them
    val len_b = ary_B.map{case (idx, line) => idx -> line.split(",").length}.collectAsMap()
    val len_b_bc = sc.broadcast(len_b.toArray)

    // Extract ("Topic", index) for Array B, e.g.: ((2,3), 0)
    val b_memberships = get_global_topic_membership(get_local_topic_membership(ary_B, "B"))

    //------------------------------------------------------------------------------------------------
    // iii) Main logic
    //------------------------------------------------------------------------------------------------
    // 1. Combine A and B through Topic, e.g.: ((2,3), idx_A, inx_B)
    val ab_memberships_ary = a_memberships.join(b_memberships)

    // 2. Explodes (Flatten) them prior to counting. e.g.: ((2,3), (idx_A_1, idx_A_2), (idx_B_1, idx_B_2)) -->
    // (idx_A_1, idx_B_1), (idx_A_1, idx_B_2), (idx_A_2, idx_B_2), (idx_A_2, idx_B_1)

    val ab_pair = explode_members(ab_memberships_ary)

    // 3. Get count of interaction between idx_A and idx_B
    val ab_pair_cocounts = cocounts(ab_pair)


    // 4. If count of interaction is equivalent to length of that B element, then that element of A is true.
    val ab_matches = get_a_b_matches(ab_pair_cocounts, len_b_bc)

    // 5. For every element of A, find if such element exists in B
    val a_bool = sum_over_b(ab_matches)

    //------------------------------------------------------------------------------------------------
    // iv) Outputing
    //------------------------------------------------------------------------------------------------
    // Format output
    val array_c = format_output(ary_A.leftOuterJoin(a_bool))

    // Writing output
    write_spark(path_c, sqlContext, array_c)
  }
}
