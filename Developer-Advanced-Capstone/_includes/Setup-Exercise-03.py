# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_e_passed = False
check_final_passed = False

source_path = f"{working_dir}/exercise_03/raw"

exp_trx_count = 0

# COMMAND ----------

def install_datasets_03(reinstall):
  global exp_trx_count
  install_exercise_datasets("exercise_03", source_path, "2 minute", "6 minutes", reinstall)
  exp_trx_count = spark.read.format("delta").load(source_path).count()

def reality_check_03_a():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_a_passed
  
  suite_name = "ex.03.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", f"Using DBR 7.3 LTS, with 8 cores", dependsOn=[suite.lastTestId()], 
             testFunction = validate_cluster)
  
  suite.test(f"{suite_name}.reg_id", f"Registration ID was specified", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_registration_id(registration_id))

  suite.test(f"{suite_name}.current-db", f"The current database is {database_name}", dependsOn=[suite.lastTestId()],
           testFunction = lambda: spark.catalog.currentDatabase() == database_name)
  
  suite.test(f"{suite_name}.root", f"Datasets: expected 33 file in '/'", dependsOn=[suite.lastTestId()],
            testFunction = lambda: validate_file_count(source_path, 33))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

step_b_file_size = 333 * 1024 * 1024
step_b_path = f"{working_dir}/exercise_03/step_b"

def show_exercise_03_b_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("step_b_file_size", step_b_file_size, """The maximum size of each part-file in Step #3.B""")
  html += html_row_var("", "", "")

  html += html_row_var("source_path", source_path, """The path to our non-optimized dataset""")
  html += html_row_var("step_b_path", step_b_path, """The path to our new, optimized dataset""")

  html += "</table></body></html>"
  displayHTML(html)

  
def reality_check_03_b():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_b_passed

  suite_name = "ex.03.b"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #3.B")
  
  def execute_solution():
    reset_environment() 
    print(f"Removing {step_b_path}")
    dbutils.fs.rm(step_b_path, True)
    print(f"Executing your solution...")
    files = dbutils.fs.ls(source_path)
    part_files_count = calculate_part_file_count(files, step_b_file_size)
    compact_dataset(source_path, step_b_path, part_files_count)
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.cpfc_128",   f"calculate_part_file_count @ 128 MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: calculate_part_file_count(dbutils.fs.ls(source_path), 128 * 1024 * 1024) == 17)
  suite.test(f"{suite_name}.cpfc_256",   f"calculate_part_file_count @ 256 MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: calculate_part_file_count(dbutils.fs.ls(source_path), 256 * 1024 * 1024) == 9)
  suite.test(f"{suite_name}.cpfc_512",   f"calculate_part_file_count @ 512 MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: calculate_part_file_count(dbutils.fs.ls(source_path), 512 * 1024 * 1024) == 5)
  suite.test(f"{suite_name}.cpfc_1024",  f"calculate_part_file_count @ 1024 MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: calculate_part_file_count(dbutils.fs.ls(source_path), 1024 * 1024 * 1024) == 3)
  
  suite.test(f"{suite_name}.exists",     f"Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(step_b_path))
  suite.test(f"{suite_name}.file_type",  f"Correct file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(step_b_path, "parquet"))
  suite.test(f"{suite_name}.part-files", f"Correct part-files count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: len(list(filter(lambda f: f.name.endswith(f".parquet"), dbutils.fs.ls(step_b_path)))) == 7)
  suite.test(f"{suite_name}.total",      f"Correct record count", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: exp_trx_count == spark.read.parquet(step_b_path).count())

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

step_c_file_size = 256 * 1024 * 1024
step_c_path = f"{working_dir}/exercise_03/step_c"

def create_step_c_dataset():
  sc.setJobDescription(f"Creating Step Step 3.C Dataset")
  dbutils.fs.rm(step_c_path, True)
  spark.read.format("delta").load(source_path).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(step_c_path)

def show_exercise_03_c_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("step_c_file_size", step_c_file_size, """The maximum size of each part-file in Step #3.C""")
  html += html_row_fun("create_step_c_dataset", """The utility function to re-create the dataset used in Step #3.C""")

  html += "</table></body></html>"
  displayHTML(html)

  print(f"Creating the dataset for this step...")
  create_step_c_dataset()
  print(f"Created the datasets {step_c_path}")
  
def reality_check_03_c():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_c_passed

  suite_name = "ex.03.c"
  suite = TestSuite()

  sc.setJobDescription("Reality Check #3.C")

  def execute_solution():
    reset_environment() 
    print(f"Removing {step_c_path}")
    dbutils.fs.rm(step_c_path, True)
    print(f"Executing your solution...")
    create_step_c_dataset()
    auto_optimize_existing_dataset(step_c_path, step_c_file_size)
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.exists",    f"Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(step_c_path))
  suite.test(f"{suite_name}.file_type", f"Correct file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(step_c_path, "delta"))

  suite.test(f"{suite_name}.writes",  f"Optimzied Writes enabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(step_c_path, "delta.autoOptimize.optimizeWrite", True))
  suite.test(f"{suite_name}.compact", f"Auto Compaction enabled", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(step_c_path, "delta.autoOptimize.autoCompact", True))
  suite.test(f"{suite_name}.max",     f"Maximum file size is 256MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: str(step_c_file_size) in spark.conf.get("spark.databricks.delta.autoCompact.maxFileSize"))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

step_d_file_size = 192 *1024 * 1024
step_d_path = f"{working_dir}/exercise_03/step_d"

def show_exercise_03_d_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("step_d_file_size", step_d_file_size, """The maximum size of each part-file in Step #3.D""")
  html += html_row_fun("create_step_d_dataset", """The utility function to create the dataset used in Step #3.D""")

  html += "</table></body></html>"
  displayHTML(html)
  
def create_step_d_dataset():
  sc.setJobDescription(f"Creating Step Step 3.D Dataset")
  dbutils.fs.rm(step_d_path, True)
  spark.read.format("delta").load(source_path).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(step_d_path)

def reality_check_03_d():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_d_passed

  suite_name = "ex.03.d"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #3.D")

  def execute_solution():
    reset_environment() 
    print(f"Removing {step_d_path}")
    dbutils.fs.rm(step_d_path, True)
    print(f"Executing your solution...")
    auto_optimize_future_datasets(step_d_file_size)
    create_step_d_dataset()
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.exists",    f"Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(step_d_path))
  suite.test(f"{suite_name}.file_type", f"Correct file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(step_d_path, "delta"))

  suite.test(f"{suite_name}.env.writes",     f"Optimzied Writes enabled for new datasets", dependsOn=[suite.lastTestId()], 
           testFunction = lambda: str(True).lower() == spark.conf.get("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite"))
  suite.test(f"{suite_name}.env.compact",    f"Auto Compaction enabled for new datasets", dependsOn=[suite.lastTestId()], 
           testFunction = lambda: str(True).lower() == spark.conf.get("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"))
  
  suite.test(f"{suite_name}.ds.writes",  f"Optimzied Writes enabled on test dataset", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(step_d_path, "delta.autoOptimize.optimizeWrite", True))
  suite.test(f"{suite_name}.ds.compact", f"Auto Compaction enabled on test dataset", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_table_property(step_d_path, "delta.autoOptimize.autoCompact", True))

  suite.test(f"{suite_name}.max",     f"Maximum file size is 192MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: str(step_d_file_size) in spark.conf.get("spark.databricks.delta.autoCompact.maxFileSize"))

  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_d_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

step_e_partition_size = 256 *1024 * 1024
step_e_path_dst = f"{working_dir}/exercise_03/step_e_dst"
step_e_path_src = f"{working_dir}/exercise_03/step_e_src"

def show_exercise_03_e_details():
  html = html_intro()
  html += html_header()

  html += html_row_var("step_e_partition_size", step_e_partition_size, """The maximum size of each part-file in Step #3.E""")
  html += html_row_var("step_e_path_dst",       step_e_path_dst,       """The location of the test dataset""")
  html += html_row_fun("create_step_e_dataset",              """The utility function to create the final dataset used in Step #3.E""")

  html += "</table></body></html>"
  displayHTML(html)

  print(f"Creating the dataset for this step...")
  create_step_e_source()
  print(f"Created the datasets {step_e_source}")
  
def create_step_e_source():
  sc.setJobDescription(f"Creating Step Step 3.E Source Dataset")
  dbutils.fs.rm(step_e_source, True)
  spark.read.format("delta").load(source_path).repartition(1).write.mode("overwrite").option("overwriteSchema", True).parquet(step_e_source)

def create_step_e_dataset():
  sc.setJobDescription(f"Creating Step Step 3.E Final Dataset")
  print(f"Writing dataset to {step_e_path_dst}")
  dbutils.fs.rm(step_e_path_dst, True)
  spark.read.format("delta").load(source_path).write.mode("overwrite").option("overwriteSchema", True).parquet(step_e_path_dst)

def reality_check_03_e():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  
  global check_e_passed

  suite_name = "ex.03.e"
  suite = TestSuite()
  
  sc.setJobDescription("Reality Check #3.E")

  def execute_solution():
    reset_environment() 
    print(f"Removing {step_e_path_dst}")
    dbutils.fs.rm(step_e_path_dst, True)
    print(f"Executing your solution...")
    compact_upon_ingest(step_e_partition_size)
    create_step_e_dataset()
    print(f"Evaluating your solution...")
    return True
    
  suite.test(f"{suite_name}.solution", f"Executed solution without exception", dependsOn=[suite.lastTestId()], 
             testFunction = execute_solution)

  suite.test(f"{suite_name}.exists",    f"Directory exists", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_dir_exists(step_e_path_dst))
  suite.test(f"{suite_name}.file_type", f"Correct file type", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_file_type(step_e_path_dst, "parquet"))
  suite.test(f"{suite_name}.max",       f"Maximum file size upon ingest is 256 MB", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: str(step_e_partition_size) in spark.conf.get("spark.sql.files.maxPartitionBytes"))
  suite.test(f"{suite_name}.count",     f"Expected 11 part-files",
            testFunction = lambda: len(list(filter(lambda f: f.name.endswith(".parquet"), dbutils.fs.ls(step_e_path_dst)))) == 11)
  
  daLogger.logSuite(suite_name, registration_id, suite)
  
  check_e_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_final():
  # Restore candidate-shadowed builtins
  from builtins import len, list, map, filter
  from pyspark.sql.functions import col, year, month, dayofmonth, from_unixtime

  global check_final_passed

  suite_name = "ex.03.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 03.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 03.B passed", check_b_passed, True)
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 03.C passed", check_c_passed, True)
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 03.D passed", check_d_passed, True)
  suite.testEquals(f"{suite_name}.e-passed", "Reality Check 03.E passed", check_e_passed, True)
  
  check_final_passed = suite.passed
    
  daLogger.logSuite(suite_name, registration_id, suite)
  
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)
 
  suite.displayResults()

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_var("source_path", source_path, """The path to this exercise's dataset""")
html += html_row_fun("install_datasets_03()", "A utility function for installing datasets into the current workspace.")

html += html_reality_check("reality_check_03_a()", "3.A")

html += "</table></body></html>"

displayHTML(html)
