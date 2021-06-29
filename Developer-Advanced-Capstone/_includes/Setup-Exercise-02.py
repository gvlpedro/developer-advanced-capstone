# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

dataset_path = f"{working_dir}/exercise_02/raw"

check_a_passed = False
check_b_passed_cty = False
check_b_passed_ret = False
check_b_passed_trx = False
check_c_passed = False
check_d_passed_ret = False
check_d_passed_trx = False
check_final_passed = False

exp_cty_count = 0
exp_ret_count = 0
exp_trx_count = 0

gold_base = f"{working_dir}/exercise_02/gold"
bucketed_ret_path = f"{gold_base}/bucketed_ret"
bucketed_trx_path = f"{gold_base}/bucketed_trx"

bucketed_ret_table = "bucketed_ret"
bucketed_trx_table = "bucketed_trx"

buckets = 24

# COMMAND ----------

def install_datasets_02(reinstall=False):
  global exp_cty_count
  global exp_ret_count
  global exp_trx_count

  install_exercise_datasets("exercise_02", dataset_path, "1 minute", "5 minutes", reinstall)
  
  exp_cty_count = spark.read.parquet(raw_cty_path).count()
  exp_ret_count = spark.read.parquet(raw_ret_path).count()
  exp_trx_count = spark.read.parquet(raw_trx_path).count()
  
def reality_check_02_a():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_a_passed
  
  suite_name = "ex.02.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", f"Using DBR 7.3 LTS, with 8 cores", dependsOn=[suite.lastTestId()], 
             testFunction = validate_cluster)
  
  suite.test(f"{suite_name}.reg_id", f"Registration ID was specified", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_registration_id(registration_id))

  suite.test(f"{suite_name}.current-db", f"The current database is {database_name}", dependsOn=[suite.lastTestId()],
           testFunction = lambda: spark.catalog.currentDatabase() == database_name)

  suite.test(f"{suite_name}.root", f"Datasets: expected 3 file in '/'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(dataset_path, 3))
  suite.test(f"{suite_name}.cities", f"Datasets: expected 4 file in '/cities.parquet'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(f"{dataset_path}/cities.parquet", 4))
  suite.test(f"{suite_name}.retailers", f"Datasets: expected 4 file in '/retailers.parquet'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(f"{dataset_path}/retailers.parquet", 4))
  suite.test(f"{suite_name}.transactions", f"Datasets: expected 23 file in '/transactions.parquet'", dependsOn=[suite.lastTestId()],
          testFunction = lambda: validate_file_count(f"{dataset_path}/transactions.parquet", 23))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

max_file_size_bytes = 512*1024*1024

raw_cty_path = f"{dataset_path}/cities.parquet"
raw_ret_path = f"{dataset_path}/retailers.parquet"
raw_trx_path = f"{dataset_path}/transactions.parquet"

bronze_base = f"{working_dir}/exercise_02/bronze"
bronze_cty_path = f"{bronze_base}/cities"
bronze_ret_path = f"{bronze_base}/retailers"
bronze_trx_path = f"{bronze_base}/transactions"


def show_exercise_02_b_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("max_file_size_bytes", max_file_size_bytes, """The maximum size of each part-file in the bronze layer, expressed in bytes""")
  html += html_row_var("", "", "")

  html += html_row_var("raw_cty_path", raw_cty_path, """The path to the raw cities dataset""")
  html += html_row_var("raw_ret_path", raw_ret_path, """The path to the raw retailers dataset""")
  html += html_row_var("raw_trx_path", raw_trx_path, """The path to the raw transactions dataset""")
  html += html_row_var("", "", "")
  
  html += html_row_var("bronze_cty_path",  bronze_cty_path,  """The path to the bronze cities dataset""")
  html += html_row_var("bronze_ret_path",  bronze_ret_path,  """The path to the bronze retailers dataset""")
  html += html_row_var("bronze_trx_path",  bronze_trx_path,  """The path to the bronze transactions dataset""")

  html += "</table></body></html>"
  displayHTML(html)
  
  
def reality_check_02_b_city():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter

  global check_b_passed_cty
  suite = TestSuite()
  suite_name = f"ex.02.b.cty"
  path = bronze_cty_path

  sc.setJobDescription("Reality Check #2.B-City")

  def execute_solution():
    reset_environment() 
    remove_delta_table_at(path)

    print(f"Executing your solution...")
    configure_bronze_job(max_part_file_size=max_file_size_bytes)
    create_bronze_dataset(src_path=raw_cty_path, dst_path=bronze_cty_path, name="Cities")
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.exists",    f"City: Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.file_type", f"City: Optimized file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(path, "delta"))
  suite.test(f"{suite_name}.stray",     f"City: No stray files", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_stray_files(path))
  suite.test(f"{suite_name}.file_size", f"City: Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_optimized_file_size(path))
  suite.test(f"{suite_name}.max_size",  f"City: Maximum part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_max_file_size(path, max_file_size_bytes))
  suite.test(f"{suite_name}.writes",    f"City: Auto-Optimization Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.optimizeWrite", [False,None]))
  suite.test(f"{suite_name}.compact",   f"City: Auto-Compaction Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.autoCompact", [False,None]))
  suite.test(f"{suite_name}.total",     f"City: Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_cty_count == spark.read.format("delta").load(path).count())

  check_b_passed_cty = suite.passed
  daLogger.logSuite(suite_name, registration_id, suite)
  suite.displayResults()

  
def reality_check_02_b_retailers():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter

  global check_b_passed_ret
  suite = TestSuite()
  suite_name = f"ex.02.b.ret"
  path = bronze_ret_path

  sc.setJobDescription("Reality Check #2.B-Retailer")

  def execute_solution():
    reset_environment() 
    remove_delta_table_at(path)

    print(f"Executing your solution...")
    configure_bronze_job(max_part_file_size=max_file_size_bytes)
    create_bronze_dataset(src_path=raw_ret_path, dst_path=bronze_ret_path, name="Retailers")
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.exists",    f"Retailer: Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.file_type", f"Retailer: Optimized file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(path, "delta"))
  suite.test(f"{suite_name}.stray",     f"Retailer: No stray files", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_stray_files(path))
  suite.test(f"{suite_name}.file_size", f"Retailer: Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_optimized_file_size(path))
  suite.test(f"{suite_name}.max_size",  f"Retailer: Maximum part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_max_file_size(path, max_file_size_bytes))
  suite.test(f"{suite_name}.writes",    f"Retailer: Auto-Optimization Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.optimizeWrite", [False,None]))
  suite.test(f"{suite_name}.compact",   f"Retailer: Auto-Compaction Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.autoCompact", [False,None]))
  suite.test(f"{suite_name}.total",     f"Retailer: Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_ret_count == spark.read.format("delta").load(path).count())

  check_b_passed_ret = suite.passed
  daLogger.logSuite(suite_name, registration_id, suite)
  suite.displayResults()
  
  
def reality_check_02_b_transactions():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_b_passed_trx
  suite = TestSuite()
  suite_name = f"ex.02.b.trx"
  path = bronze_trx_path

  def execute_solution():
    reset_environment() 
    remove_delta_table_at(path)

    print(f"Executing your solution...")
    configure_bronze_job(max_part_file_size=max_file_size_bytes)
    create_bronze_dataset(src_path=raw_trx_path, dst_path=path, name="Transactions")
    print(f"Evaluating your solution...")
    return True

  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)
  
  sc.setJobDescription("Reality Check #2.B-Transactions")
  suite.test(f"{suite_name}.exists",    f"Transactions: Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.file_type", f"Transactions: Optimized file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(path, "delta"))
  suite.test(f"{suite_name}.stray",     f"Transactions: No stray files", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_stray_files(path))
  suite.test(f"{suite_name}.file_size", f"Transactions: Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_optimized_file_size(path))
  suite.test(f"{suite_name}.max_size",  f"Transactions: Maximum part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_max_file_size(path, max_file_size_bytes))
  suite.test(f"{suite_name}.writes",    f"Transactions: Auto-Optimization Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.optimizeWrite", [False,None]))
  suite.test(f"{suite_name}.compact",   f"Transactions: Auto-Compaction Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.autoCompact", [False,None]))
  suite.test(f"{suite_name}.total",     f"Transactions: Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_trx_count == spark.read.format("delta").load(path).count())

  check_b_passed_trx = suite.passed
  daLogger.logSuite(suite_name, registration_id, suite)
  suite.displayResults()

# COMMAND ----------

silver_base = f"{working_dir}/exercise_02/silver"
silver_trx_cty_path = f"{silver_base}/denormalized_trx_cty"

def show_exercise_02_c_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("max_file_size_bytes", max_file_size_bytes, """The maximum size of each part-file in the bronze layer, expressed in bytes""")
  html += html_row_var("", "", "")
  
  html += html_row_var("bronze_cty_path",  bronze_cty_path,  """The path to the bronze cities dataset""")
  html += html_row_var("bronze_trx_path",  bronze_trx_path,  """The path to the bronze transactions dataset""")
  html += html_row_var("", "", "")

  html += html_row_var("silver_trx_cty_path", silver_trx_cty_path, """The path to the silver, denormalized, transactions & cities dataset""")
  
  html += "</table></body></html>"
  displayHTML(html)
  
test_df = None

def reality_check_02_c():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter

  global check_c_passed
  sc.setJobDescription("Reality Check #2.C")

  suite_name = f"ex.02.c"
  suite = TestSuite()
  path = silver_trx_cty_path
  
  def execute_solution():
    global test_df
    reset_environment() 
    remove_delta_table_at(path)

    print(f"Executing your solution...")
    configure_silver_job(max_part_file_size=max_file_size_bytes)
    test_df = create_silver_trx_cty_df(cty_src_path=bronze_cty_path, trx_src_path=bronze_trx_path)
    write_silver_trx_cty(df=test_df, dst_path=path)
    print(f"Evaluating your solution...")
    return True

  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)
  
  suite.test(f"{suite_name}.exists",    f"Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.file_type", f"Optimized file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(path, "delta"))
  suite.test(f"{suite_name}.stray",     f"No stray files", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_stray_files(path))
  suite.test(f"{suite_name}.file_size", f"Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_optimized_partition_size(path))
  suite.test(f"{suite_name}.max_size",  f"Maximum part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_max_file_size(path, max_file_size_bytes))
  suite.test(f"{suite_name}.writes",    f"Auto-Optimization Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.optimizeWrite", [False,None]))
  suite.test(f"{suite_name}.compact",   f"Auto-Compaction Disabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(path, "delta.autoOptimize.autoCompact", [False,None]))
  suite.test(f"{suite_name}.total",     f"Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_trx_count == spark.read.format("delta").load(path).count())
  
  suite.test(f"{suite_name}.index_cty", f"High-Cardinality index on City ID", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_zordered_by_any(get_delta_history(path, "OPTIMIZE", "operationParameters.zOrderBy"), ["city_id", "z_city_id"]))
  suite.test(f"{suite_name}.index_trx", f"High-Cardinality index on Transaction ID", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_zordered_by_any(get_delta_history(path, "OPTIMIZE", "operationParameters.zOrderBy"), ["trx_id", "z_trx_id"]))
  suite.test(f"{suite_name}.index_ret", f"High-Cardinality index on Retailer ID", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_zordered_by_any(get_delta_history(path, "OPTIMIZE", "operationParameters.zOrderBy"), ["retailer_id", "z_retailer_id"]))

  suite.test(f"{suite_name}.index_year", f"Low-Cardinality index on Year", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_partitioned_by_any(get_delta_history(path, "WRITE", "operationParameters.partitionBy"), ["year", "p_year"]))
  
  suite.test(f"{suite_name}.index_all", f"Advertised all indexes", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_columns_exist(path_to_table("delta", path), ["z_trx_id", "z_city_id", "z_retailer_id", "p_year"]))
  
  suite.test(f"{suite_name}.city_id", f"Join: No duplicate columns", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: spark.read.format("delta").load(path).select("z_city_id"))
  
  suite.test(f"{suite_name}.join", f"Join: Properly optimized", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_broadcasted(test_df))
  
  suite.test(f"{suite_name}.skew", f"Join: Managed skew", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_aqe_skew_join())

  daLogger.logSuite(suite_name, registration_id, suite)
  
  suite.displayResults()
  check_c_passed = suite.passed

# COMMAND ----------

def show_exercise_02_d_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("buckets", buckets, """The number of buckets by which these tables will be partitioned""")
  html += html_row_var("", "", "")

  html += html_row_var("bronze_ret_path", bronze_ret_path, """The path to the bronze retailers dataset""")
  html += html_row_var("bucketed_ret_path", bucketed_ret_path, """The path to the gold, bucketed, retailers dataset""")
  html += html_row_var("bucketed_ret_table", bucketed_ret_table, """The name of the bucketed retailers table""")
  html += html_row_var("", "", "")

  html += html_row_var("bronze_trx_path", bronze_trx_path, """The path to the bronze transactions dataset""")
  html += html_row_var("bucketed_trx_path", bucketed_trx_path, """The path to the gold, bucketed, transactions dataset""")
  html += html_row_var("bucketed_trx_table", bucketed_trx_table, """The name of the bucketed transactions table""")
  
  html += "</table></body></html>"
  displayHTML(html)
  
  
def reality_check_02_d_retailers():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_d_passed_ret

  suite_name = "ex.02.d.ret"
  suite = TestSuite()
  path = bucketed_ret_path
  table = bucketed_ret_table

  sc.setJobDescription("Reality Check #2.D-Retailers")
  
  def execute_solution():
    reset_environment() 
    print(f"Dropping {table}...")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Executing your solution...")
    bucket_dataset(bronze_ret_path, path, table, buckets, "Retailers")
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)
  
  suite.test(f"{suite_name}.exists",    f"Retailer: Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.table",     f"Retailer: Table exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_exists(table))
  suite.test(f"{suite_name}.file_size", f"Retailer: Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: len(list(filter(lambda f: f.name.endswith(".parquet"), dbutils.fs.ls(path)))) == 24)
  suite.test(f"{suite_name}.bucketed",  f"Retailer: Bucketed by Retailer Id", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_bucketed_by_any(table, ["retailer_id", "b_retailer_id"]))
  suite.test(f"{suite_name}.index_all", f"Retailer: Advertised all indexes", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_columns_exist(table, ["b_retailer_id"]))
  suite.test(f"{suite_name}.buckets",   f"Retailer: Bucketed with N buckets", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_num_buckets(table, 24))
  suite.test(f"{suite_name}.total",     f"Retailer: Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_ret_count == spark.read.table(table).count())
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed_ret = suite.passed
  suite.displayResults()

  
def reality_check_02_d_transactions():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_d_passed_trx

  suite_name = "ex.02.d.trx"
  suite = TestSuite()
  path = bucketed_trx_path
  table = bucketed_trx_table

  sc.setJobDescription("Reality Check #2.D-Transactions")
  
  def execute_solution():
    reset_environment() 
    
    print(f"Dropping {table}...")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    
    print(f"Executing your solution...")
    bucket_dataset(bronze_trx_path, path, table, buckets, "Transactions")
    print(f"Testing the join operation...")
    spark.read.table(bucketed_ret_table).join(spark.read.table(bucketed_trx_table), "b_retailer_id").write.format("noop").mode("overwrite").save()
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)
  
  suite.test(f"{suite_name}.exists",    f"Transactions: Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(path))
  suite.test(f"{suite_name}.table",     f"Transactions: Table exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_exists(table))
  suite.test(f"{suite_name}.file_size", f"Transactions: Optimized part-file size", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: len(list(filter(lambda f: f.name.endswith(".parquet"), dbutils.fs.ls(path)))) == 24)
  suite.test(f"{suite_name}.bucketed",  f"Transactions: Bucketed by Retailer Id", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_bucketed_by_any(table, ["retailer_id", "b_retailer_id"]))
  suite.test(f"{suite_name}.index_all", f"Transactions: Advertised all indexes", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_columns_exist(table, ["b_retailer_id"]))
  suite.test(f"{suite_name}.buckets",   f"Transactions: Bucketed with 24 buckets", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_num_buckets(table, 24))
  suite.test(f"{suite_name}.total",     f"Transactions: Correct Record Count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_trx_count == spark.read.table(table).count())

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed_trx = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_final():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  from pyspark.sql.functions import col, year, month, dayofmonth, from_unixtime

  global check_final_passed
  
  suite_name = "ex.02.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 02.A", check_a_passed, True)
  
  suite.testEquals(f"{suite_name}.b-passed-cty", "Reality Check 02.B-City",         check_b_passed_cty, True)
  suite.testEquals(f"{suite_name}.b-passed-ret", "Reality Check 02.B-Retailer",     check_b_passed_ret, True)
  suite.testEquals(f"{suite_name}.b-passed-trx", "Reality Check 02.B-Transactions", check_b_passed_trx, True)
  
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 02.C", check_c_passed, True)

  suite.testEquals(f"{suite_name}.d-passed-ret", "Reality Check 02.D-Retailer",     check_d_passed_ret, True)
  suite.testEquals(f"{suite_name}.d-passed-trx", "Reality Check 02.D-Transactions", check_d_passed_trx, True)
  
  check_final_passed = suite.passed
    
  daLogger.logSuite(suite_name, registration_id, suite)
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
 
  suite.displayResults()

# COMMAND ----------

html = html_intro()
html += html_header()
html += html_row_fun("install_datasets_02()", "A utility function for installing datasets into the current workspace.")
html += html_reality_check("reality_check_02_a()", "2.A")
html += "</table></body></html>"
displayHTML(html)
