# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

check_final_passed = False
dataset_path = f"{working_dir}/exercise_01/raw"

# COMMAND ----------

def install_datasets_01(reinstall):
  install_exercise_datasets("exercise_01", dataset_path, "1 second", "5 seconds", reinstall=reinstall)

def reality_check_01_a():
  global check_final_passed
  
  suite_name = "ex.01.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", f"Using DBR 7.3 LTS, with 4 or 8 cores", dependsOn=[suite.lastTestId()], 
             testFunction = validate_cluster)
  
  suite.test(f"{suite_name}.reg_id", f"Registration ID was specified", dependsOn=[suite.lastTestId()], 
             testFunction = lambda: validate_registration_id(registration_id))

  suite.test(f"{suite_name}.current-db", f"The current database is {database_name}", dependsOn=[suite.lastTestId()],
           testFunction = lambda: spark.catalog.currentDatabase() == database_name)
  
  test_1_count = len(dbutils.fs.ls(dataset_path+"/"))
  suite.testEquals(f"{suite_name}.root", f"Datasets: expected 1 file, found {test_1_count} in '/'", 1, test_1_count, dependsOn=[suite.lastTestId()])

  check_final_passed = suite.passed
  
  daLogger.logSuite(suite_name, registration_id, suite)
  daLogger.logAggregatedResults(getLessonName(), registration_id, TestResultsAggregator)

  suite.displayResults()  
  
None # Suppress output

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_var("dataset_path", dataset_path, "The path to this exercise's dataset")
html += html_row_fun("install_datasets_01()", "A utility function for installing datasets into the current workspace.")

html += html_row_fun("reality_check_01()", "A utility function for validating your configuration.")

html += "</table></body></html>"

displayHTML(html)
