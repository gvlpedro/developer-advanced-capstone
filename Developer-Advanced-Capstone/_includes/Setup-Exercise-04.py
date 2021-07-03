# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

sc.setJobDescription("Setting up Exercise #5")

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_final_passed = False

baseline_min = 0
baseline_avg = 0
baseline_max = 0
baseline_results = None
default_partitions = 0

source_path = f"{working_dir}/exercise_04/raw"
cty_src_path = f"{source_path}/cities.parquet"
trx_src_path = f"{source_path}/transactions.parquet"

def print_results(results):
  for i, result in enumerate(results): 
    print("#{}: {:,}".format(i, result))

def records_per_partition(df):
  from pyspark.sql.functions import spark_partition_id
  results = df.withColumn("pid", spark_partition_id()).groupBy("pid").count().drop("pid").collect()
  return list(map(lambda r: r["count"], results))

def print_statistics(results, label="Partition Statistics"):
  import statistics
  non_zero_results = list(filter(lambda r: r>0, results))
  print(label)
  print(f" - Minimum: {__builtin__.min(non_zero_results):>9,d}")
  print(f" - Average: {int(__builtin__.sum(non_zero_results)/len(non_zero_results)):>9,d}")
  print(f" - Median:  {int(statistics.median(non_zero_results)):>9,d}")
  print(f" - Maximum: {__builtin__.max(non_zero_results):>9,d}")
  print(f" - Count:   {__builtin__.len(non_zero_results):>9,d}")
  print(f" - Non-Zero:{__builtin__.len(results):>9,d}")
  
def divergence(max_value, avg_value):
  if avg_value == 0: return 0
  else: return max_value/avg_value

# COMMAND ----------

def install_datasets_04(reinstall):
  global baseline_min
  global baseline_avg
  global baseline_max
  global baseline_results
  global default_partitions
  
  install_exercise_datasets("exercise_04", source_path, "2 minute", "6 minutes", reinstall)
  
  if default_partitions == 0:
    sc.setJobDescription("Computing Partitions")
    print("\nComputing some default number of partitions, please wait...")
    # default_partitions = spark.read.parquet(trx_src_path).rdd.getNumPartitions()
    # default_partitions = spark.read.parquet(cty_src_path).count()
    default_partitions = spark.read.parquet(trx_src_path).select("city_id").distinct().count()


  sc.setJobDescription("Computing baseline")
  print("\nComputing baseline, please wait...")
  spark.conf.set("spark.sql.adaptive.enabled", False)
  spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", False)
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.shuffle.partitions", default_partitions)
  baseline_results = records_per_partition(
    spark.read.parquet(trx_src_path).join(spark.read.parquet(cty_src_path), "city_id")
  )
  baseline_min = __builtin__.min(baseline_results)
  baseline_avg = int(__builtin__.sum(baseline_results)/len(baseline_results))
  baseline_max = __builtin__.max(baseline_results)
    
  print()
  print_statistics(baseline_results, label="Baseline Partition Statistics")
  
  print()
  reset_environment()

def reality_check_04_a():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_a_passed
  
  suite_name = "ex.04.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", f"Using DBR 7.3 LTS, with 4 or 8 cores", dependsOn=[suite.lastTestId()], 
             testFunction = validate_cluster)
  
  suite.test(f"{suite_name}.reg_id", f"Registration ID was specified", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_registration_id(registration_id))

  suite.test(f"{suite_name}.current-db", f"The current database is {database_name}", dependsOn=[suite.lastTestId()],
           testFunction = lambda: spark.catalog.currentDatabase() == database_name)
  
  suite.test(f"{suite_name}.root", f"Datasets: expected 2 file in '/'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(source_path, 2))
  suite.test(f"{suite_name}.cities", f"Datasets: expected 4 file in '/cities.parquet'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(cty_src_path, 4))
  suite.test(f"{suite_name}.transactions", f"Datasets: expected 35 file in '/transactions.parquet'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(trx_src_path, 35))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

e5b_join_column = None
e5b_skewed_partition_factor = 0
e5b_partition_size = 0

test_b_df = None
test_b_results = None
test_b_min = 0
test_b_avg = 0
test_b_max = 0

def show_exercise_04_b_details():
  global e5b_join_column
  global e5b_skewed_partition_factor
  global e5b_partition_size

  e5b_join_column = "city_id"
  e5b_skewed_partition_factor = 2
  e5b_partition_size = 50*1024*1024
  
  html = html_intro()
  html += html_header()

  html += html_row_var("e5b_join_column",  e5b_join_column,  """The column by which the two datasets will be joined""")
  html += html_row_var("e5b_partition_size",  e5b_partition_size,  """Both the Skewed Partition Threshold and Advisory Partition Size in bytes""")
  html += html_row_var("e5b_skewed_partition_factor",  e5b_skewed_partition_factor,  """The Skewed Partition Factor""")
  html += html_row_var("", "", "")

  html += html_row_var("cty_src_path", cty_src_path, """The path to the cities dataset""")
  html += html_row_var("trx_src_path", trx_src_path, """The path to the transactions dataset""")
  html += html_row_var("", "", "")

  html += html_row_fun("records_per_partition()", "A utility function to count the number of records in each partition.")
  html += html_row_fun("print_statistics()", "Print the statistics of the result returned by records_per_partition().")
  html += html_reality_check("reality_check_04_b()", "5.B")
  
  html += "</table></body></html>"
  displayHTML(html)  

def reset_environment_04_b():
  reset_environment() 

  print(f"Disabling auto broadcasting")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  print(f"Setting spark.sql.shuffle.partitions to {default_partitions}")
  spark.conf.set("spark.sql.shuffle.partitions", default_partitions)

  
def reality_check_04_b():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  global check_b_passed

  suite_name = "ex.04.b"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #4.B")
  
  def execute_solution():
    global test_b_df, test_b_results, test_b_min, test_b_avg, test_b_max
    try:
      reset_environment_04_b()

      print(f"Executing your solution...")
      test_b_df = skew_join_with_aqe(trx_src_path, cty_src_path, e5b_join_column, e5b_skewed_partition_factor, e5b_partition_size)
      test_b_results = records_per_partition(test_b_df)
      print_statistics(test_b_results)

      test_b_min = __builtin__.min(test_b_results)
      test_b_avg = int(__builtin__.sum(test_b_results)/len(test_b_results))
      test_b_max = __builtin__.max(test_b_results)
      print(f"\nEvaluating your solution...")
      return True
    except Exception as e:
      print(e)
      return False
    
  solution_b = execute_solution()  
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: solution_b)
  
  suite.test(f"{suite_name}.aqe.enabled", f"Adaptive Query Execution enabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "true" == str(spark.conf.get("spark.sql.adaptive.enabled")).lower())
  
  suite.test(f"{suite_name}.join.enabled", f"Skew Join enabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "true" == str(spark.conf.get("spark.sql.adaptive.skewJoin.enabled")).lower())

  suite.test(f"{suite_name}.partition.factor", f"Correct partition factor", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "2" == str(spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")).lower())

  suite.test(f"{suite_name}.partition.threshold", f"Correct partition threshold", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: str(e5b_partition_size) in str(spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")).lower())

  suite.test(f"{suite_name}.advisory", f"Correct advisory partition size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: str(e5b_partition_size) in str(spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes")).lower())

  suite.test(f"{suite_name}.div", f"Improved average's divergence from maximum (from {divergence(baseline_max, baseline_avg):,.4f} to {divergence(test_b_max, test_b_avg):,.4f})", dependsOn=[suite.lastTestId()],
             testFunction = lambda: divergence(test_b_max, test_b_avg) < divergence(baseline_max, baseline_avg))
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

e5c_join_column = None

test_c_df = None
test_c_results = None
test_c_min = 0
test_c_avg = 0
test_c_max = 0

def show_exercise_04_c_details():
  global e5c_join_column
  
  e5c_join_column = "city_id"
  
  html = html_intro()
  html += html_header()

  html += html_row_var("e5c_join_column",  e5c_join_column,  """The column by which the two datasets will be joined""")
  html += html_row_var("", "", "")

  html += html_row_var("cty_src_path", cty_src_path, """The path to the cities dataset""")
  html += html_row_var("trx_src_path", trx_src_path, """The path to the transactions dataset""")
  html += html_row_var("", "", "")

  html += html_row_fun("records_per_partition()", "A utility function to count the number of records in each partition.")
  html += html_row_fun("print_statistics()", "Print the statistics of the result returned by records_per_partition().")
  html += html_reality_check("reality_check_04_c()", "5.B")

  html += "</table></body></html>"
  displayHTML(html)  

def reset_environment_04_c():
    reset_environment() 

    print(f"Disabling the AQE framework")
    spark.conf.set("spark.sql.adaptive.enabled", False)

    print(f"Disabling auto broadcasting")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    print(f"Setting spark.sql.shuffle.partitions to {default_partitions}")
    spark.conf.set("spark.sql.shuffle.partitions", default_partitions)

def reality_check_04_c():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  global check_c_passed

  suite_name = "ex.04.c"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #4.C")
  
  def execute_solution():
    global test_c_df, test_c_results, test_c_min, test_c_avg, test_c_max
    try:
      reset_environment_04_c()

      print(f"Executing your solution...")
      test_c_df = hinted_skew_join(trx_src_path, cty_src_path, e5c_join_column)
      test_c_results = records_per_partition(test_c_df)
      print_statistics(test_c_results)

      test_c_min = __builtin__.min(test_c_results)
      test_c_avg = int(__builtin__.sum(test_c_results)/len(test_c_results))
      test_c_max = __builtin__.max(test_c_results)
      print(f"\nEvaluating your solution...")
      return True
    except Exception as e:
      print(e)
      return False
    
  solution_c = execute_solution()  
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: solution_c)

  suite.test(f"{suite_name}.aqe.enabled", f"Adaptive Query Execution DISABLED", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "false" == str(spark.conf.get("spark.sql.adaptive.enabled")).lower())

  suite.test(f"{suite_name}.hint", "Skew Hint Detected", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "__skewGenCol" in explain_data_frame(test_c_df) and "Generate explode(CASE WHEN city_id" in explain_data_frame(test_c_df))
  
  suite.test(f"{suite_name}.div", f"Improved average's divergence from maximum (from {divergence(baseline_max, baseline_avg):,.4f} to {divergence(test_c_max, test_c_avg):,.4f})", dependsOn=[suite.lastTestId()],
             testFunction = lambda: divergence(test_c_max, test_c_avg) < divergence(baseline_max, baseline_avg))
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

e5d_skew_factor = 0
e5d_cty_partitions = 0
e5d_salt_column = None
e5d_join_column = None

test_d_salts_df = None
test_d_salted_cty_df = None
test_d_salted_trx_df = None
test_d_df = None
test_d_results = None
test_d_min = 0
test_d_avg = 0
test_d_max = 0

def setup_exercise_04_d():
  import math
  global e5d_skew_factor
  global e5d_salt_column
  global e5d_join_column
  global e5d_cty_partitions

  e5d_skew_factor = 16
  e5d_salt_column = "salt"
  e5d_join_column = "salted_city_id"
  
  file_size = sum(list(map(lambda f: f.size, dbutils.fs.ls(cty_src_path))))
  file_size_mb = file_size/1024/1024
  cross_join_size_mb = e5d_skew_factor * file_size_mb
  e5d_cty_partitions = math.ceil(cross_join_size_mb / 128)

def show_exercise_04_d_details():
  setup_exercise_04_d()
  
  html = html_intro()
  html += html_header()
  
  html += html_row_var("e5d_skew_factor",  e5d_skew_factor,  """The pre-determined skew-factor from which all salt values will be generated""")
  html += html_row_var("e5d_salt_column",  e5d_salt_column,  """The name of the column that will contain the salt value""")
  html += html_row_var("e5d_join_column",  e5d_join_column,  """The new, salted column by which the two datasets will be joined""")
  html += html_row_var("e5d_cty_partitions",  e5d_cty_partitions,  """The number of partitions by which the salted cities dataset should be repartioned by""")
  html += html_row_var("", "", "")

  html += html_row_var("cty_src_path", cty_src_path, """The path to the cities dataset""")
  html += html_row_var("trx_src_path", trx_src_path, """The path to the transactions dataset""")
  html += html_row_var("", "", "")

  html += html_row_fun("records_per_partition()", "A utility function to count the number of records in each partition.")
  html += html_row_fun("print_statistics()", "Print the statistics of the result returned by records_per_partition().")
  html += html_reality_check("reality_check_04_d()", "5.B")

  html += "</table></body></html>"
  displayHTML(html)  

def reset_environment_04_d():
    reset_environment() 

    print(f"Disabling the AQE framework")
    spark.conf.set("spark.sql.adaptive.enabled", False)

    print(f"Disabling auto broadcasting")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    print(f"Setting spark.sql.shuffle.partitions to {default_partitions}")
    spark.conf.set("spark.sql.shuffle.partitions", default_partitions)

def reality_check_04_d():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  global check_d_passed

  suite_name = "ex.04.d"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #4.B")
  
  def execute_solution():
    global test_d_df, test_d_results, test_d_min, test_d_avg, test_d_max
    global test_d_salts_df, test_d_salted_cty_df, test_d_salted_trx_df

    try:
      reset_environment_04_d()
      
      print(f"Executing your solution...")
      test_d_salts_df = salts(e5d_skew_factor, e5d_salt_column)
      test_d_salted_cty_df = salt_cities_dataset(test_d_salts_df, cty_src_path, e5d_cty_partitions, e5d_salt_column, e5d_join_column)
      test_d_salted_trx_df = salt_transactions_dataset(e5d_skew_factor, trx_src_path, e5d_salt_column, e5d_join_column)
      test_d_df = salted_join(test_d_salted_trx_df, test_d_salted_cty_df, e5d_join_column)
      test_d_results = records_per_partition(test_d_df)
      print_statistics(test_d_results)

      test_d_min = __builtin__.min(test_d_results)
      test_d_avg = int(__builtin__.sum(test_d_results)/len(test_d_results))
      test_d_max = __builtin__.max(test_d_results)
      print(f"\nEvaluating your solution...")
      return True
    except Exception as e:
      print(e)
      return False
    
  solution_d = execute_solution()
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: solution_d)

  suite.test(f"{suite_name}.aqe.enabled", f"Adaptive Query Execution DISABLED", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: "false" == str(spark.conf.get("spark.sql.adaptive.enabled")).lower())

  
  
  suite.test(f"{suite_name}.salts",      f"Expected {e5d_skew_factor:,d} records from salts(..)", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: test_d_salts_df.count() == e5d_skew_factor)
  
  cty_count = spark.read.parquet(cty_src_path).count() * e5d_skew_factor
  suite.test(f"{suite_name}.salt_cities_dataset", f"Expected {cty_count:,d} records from salt_cities_dataset(..)", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: test_d_salted_cty_df.count() == cty_count)
  
  trx_count = spark.read.parquet(trx_src_path).count()
  suite.test(f"{suite_name}.salt_transactions_dataset", f"Expected {trx_count:,d} records from salt_transactions_dataset(..)", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: test_d_salted_trx_df.count() == trx_count)
  
  suite.test(f"{suite_name}.salted_join", f"Expected {trx_count:,d} records from salted_join(..)", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: test_d_df.count() == trx_count)
  

  
  suite.test(f"{suite_name}.div", f"Improved average's divergence from maximum (from {divergence(baseline_max, baseline_avg):,.4f} to {divergence(test_d_max, test_d_avg):,.4f})", dependsOn=[suite.lastTestId()],
             testFunction = lambda: divergence(test_d_max, test_d_avg) < divergence(baseline_max, baseline_avg))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_04_final():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  from pyspark.sql.functions import col, year, month, dayofmonth, from_unixtime

  global check_final_passed

  suite_name = "ex.04.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 04.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 04.B passed", check_b_passed, True)
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 04.C passed", check_c_passed, True)
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 04.D passed", check_d_passed, True)
  
  check_final_passed = suite.passed
    
  daLogger.logSuite(suite_name, registration_id, suite)
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
 
  suite.displayResults()

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_fun("install_datasets_04()", "A utility function for installing datasets into the current workspace.")
html += html_reality_check("reality_check_04_a()", "5.A")

html += "</table></body></html>"

displayHTML(html)
