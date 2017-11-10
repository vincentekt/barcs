Commands
1. ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_a --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local
2. ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_b --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local
3. ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_c --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local --fs local
4. ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_d --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local

