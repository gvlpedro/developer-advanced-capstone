# Databricks notebook source
sc.setJobDescription("Setting up environment")

# Get all tags
def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if len(values) > 0:
      return values
  except:
    return defaultValue

def getLessonName() -> str:
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

padding = "{padding}"
border_color = "#CDCDCD"
font = "font-size:16px;"
weight = "font-weight:bold;"
align = "vertical-align:top;"
border = f"border-bottom:1px solid {border_color};"

def html_intro():
  return """<html><body><table style="width:100%">
            <p style="font-size:16px">The following variables and functions have been defined for you.</p>"""

def html_row_var(name, value, description):
  return f"""<tr><td style="{font} {weight} {padding} {align} white-space:nowrap; color:green;">{name}</td>
                 <td style="{font} {weight} {padding} {align} white-space:nowrap; color:blue;">{value}</td>
             </tr><tr><td style="{border} {font} {padding}">&nbsp;</td>
                 <td style="{border} {font} {padding} {align}">{description}</td></tr>"""

def html_row_fun(name, description):
  return f"""<tr><td style="{border}; {font} {padding} {align} {weight} white-space:nowrap; color:green;">{name}</td>
                 <td style="{border}; {font} {padding} {align}">{description}</td></td></tr>"""

def html_header():
  return f"""<tr><th style="{border} padding: 0 1em 0 0; text-align:left">Variable/Function</th>
                 <th style="{border} padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

def html_username():
  return html_row_var("username", username, """This is the email address that you signed into Databricks with""")

def html_working_dir():
  return html_row_var("working_dir", working_dir, """This is the directory in which all work should be conducted""")

def html_database_name():
  return html_row_var("database_name", database_name, """The name of the database you will use for this project.""")

def html_orders_table():
  return html_row_var("orders_table", orders_table, """The name of the orders table.""")

def html_sales_reps_table():
  return html_row_var("sales_reps_table", sales_reps_table, """The name of the sales reps table.""")

def html_products_table():
  return html_row_var("products_table", products_table, """The name of the products table.""")

def html_line_items_table():
  return html_row_var("line_items_table", line_items_table, """The name of the line items table.""")

batch_path_desc = """The location of the combined, raw, batch of orders."""
def html_batch_source_path():
  return html_row_var("batch_source_path", batch_target_path, batch_path_desc)
def html_batch_target_path():
  return html_row_var("batch_target_path", batch_target_path, batch_path_desc)

def html_reality_check_final(fun_name):
  return html_row_fun(fun_name, """A utility function for validating the entire exercise""")
  
def html_reality_check(fun_name, exercise):
  return html_row_fun(fun_name, f"""A utility function for validating Exercise #{exercise}""")

def checkSchema(schemaA, schemaB, keepOrder=True, keepNullable=False): 
  # Usage: checkSchema(schemaA, schemaB, keepOrder=false, keepNullable=false)
  
  from pyspark.sql.types import StructField
  
  if (schemaA == None and schemaB == None):
    return True
  
  elif (schemaA == None or schemaB == None):
    return False
  
  else:
    schA = schemaA
    schB = schemaB

    if (keepNullable == False):  
        schA = [StructField(s.name, s.dataType) for s in schemaA]
        schB = [StructField(s.name, s.dataType) for s in schemaB]
  
    if (keepOrder == True):
      return [schA] == [schB]
    else:
      return set(schA) == set(schB)

None # Suppress output

# COMMAND ----------

try: original_environment
except: original_environment = dict()

environment_keys = [
  "spark.sql.files.maxPartitionBytes",
  "spark.sql.shuffle.partitions",
  
  "spark.sql.adaptive.advisoryPartitionSizeInBytes",
  "spark.sql.adaptive.coalescePartitions.enabled",
  "spark.sql.adaptive.localShuffleReader.enabled",
  "spark.sql.adaptive.enabled",
  "spark.sql.adaptive.skewJoin.enabled",
  "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
  "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
  
  "spark.databricks.delta.optimizeWrite.enabled",
  "spark.databricks.delta.autoCompact.enabled",
  "spark.databricks.delta.autoCompact.maxFileSize",
  "spark.databricks.delta.optimize.maxFileSize",
  "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
  "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
  "spark.databricks.delta.retentionDurationCheck.enabled",
]

# Capture the current environment
for key in environment_keys:
  if key not in original_environment: 
    try: 
      value = spark.conf.get(key)
      original_environment[key] = value
    except: pass

def reset_environment():
  print("Restoring the Spark configuration settings...")
  for key in environment_keys:
    if key in original_environment:
      value = original_environment[key]
      if value: spark.conf.set(key, value)

None # Suppress output

# COMMAND ----------

def get_delta_history(path, operation, key):
  try:
    alias = key.split(".")[-1]
    return (spark
      .sql(f"DESCRIBE HISTORY delta.`{path}`")
      .orderBy(FA.col("version").desc())
      .filter(FA.col("operation") == operation)
      .select(FA.col(key).alias(alias))
    ).collect()
  except Exception as e:
    print(f"**** DEBUG: Describing history for {path}")
    print(e)
    return []

def path_to_table(format, path):
  return f"{format}.`{path}`"

def explain_data_frame(data_frame):
  from io import StringIO
  import sys
  old_stdout = sys.stdout
  try:
    sys.stdout = StringIO()
    print("Testing 123")
    data_frame.explain()
    return sys.stdout.getvalue()
  except:
    return None
  finally:
    sys.stdout = old_stdout

None # Suppress output

# COMMAND ----------

def remove_delta_table_at(path):
  import time
  print(f"Removing {path}")
  
  try:
    spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
  except:
    print(f"**** DEBUG: Failed to drop table at {path}")
  
  try:
    dbutils.fs.rm(path, True)
  except:
    print(f"**** DEBUG: Failed to delete directory at {path}")
  
  # Give us 5 seconds to deal with
  # eventual consistency issues
  time.sleep(5)

def validate_file_count(path, expected):
  files = dbutils.fs.ls(path)
  if len(files) == expected: return True
  else: raise Exception(f"Expected {expected} files, found {len(files)} in {path}")
  
def validate_cluster():
  current_version = getTag("sparkVersion")
  if not current_version:
    raise Exception("The current DBR version is not yet available to this notebook.\nGive it a second and try again!")
  else:
    return current_version == "7.3.x-scala2.12" and sc.defaultParallelism == 8
    
def validate_registration_id(registration_id):
  try: 
    return int(registration_id) > 0
  except: 
    return False
    
def validate_file_type(path, file_type):
  
  files = dbutils.fs.ls(path)
  
  def is_delta():
    return len(list(filter(lambda f: f.name == "_delta_log/", files))) == 1
    
  def is_type(test_files, test_type):
    for file in test_files:
      if file.size == 0 and file.name.endswith("/") and file.name != "_delta_log/":
        if is_type(dbutils.fs.ls(file.path), test_type): 
          return True
        
      elif file.name.endswith(f".{test_type}"):
        return True
    
    return False
    
  if file_type == "delta":
    return is_delta() and is_type(files, "parquet")
  else:
    return not is_delta() and is_type(files, file_type)
  
def validate_dir_exists(path):
  try:
    return len(list(dbutils.fs.ls(path))) > 0
  except:
    return False
  
def validate_max_file_size(path, max_bytes):
  if not str(spark.conf.get("spark.databricks.delta.optimize.maxFileSize")) == str(max_bytes):
    return False

  large_files = 0
  for file in list(filter(lambda f: f.name.endswith(".parquet"), dbutils.fs.ls(path))):
    if file.size > max_bytes: 
      large_files += 1

  if large_files == 0: return True
  raise Exception(f"Found {large_files} files > {max_bytes} bytes in {path}")
  

  
def validate_optimized_file_size(path):
  tiny_files = 0
  for file in list(filter(lambda f: f.name.endswith(".parquet"), dbutils.fs.ls(path))):
    if file.size < 128 * 1024 * 1024: 
      tiny_files += 1

  if tiny_files <= 1: return True
  raise Exception(f"Found {tiny_files} tiny files in {path}")

def validate_optimized_partition_size(path):
  passed = True
  files = dbutils.fs.ls(path)
  
  for file in files:
    if file.name != "_delta_log/" and not validate_optimized_file_size(file.path):
      passed = False
  
  return passed

def validate_table_property(path, property_name, expected):
  if expected is None:
    expected_values = [None]
  elif type(expected) == list:
    expected_values = expected
  else:
    expected_values = [expected]

  passed = False
  for expected_value in expected_values:
    expected_value = expected_value if not type(expected_value) == bool else str(expected_value).lower()
    df = spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`")
    if expected_value is None:
      passed = True if df.filter(f"key = '{property_name}'").count() == 0 else passed
        
    else:
      passed = True if df.filter(f"key = '{property_name}' and value = '{expected_value}'").count() == 1 else passed

  return passed

def validate_stray_files(src_path):
  def copy_stray_files(src_path, tmp_path):
    for file in dbutils.fs.ls(src_path):
      if file.name == "_delta_log/": 
        pass
      elif file.name.endswith(".parquet"):
        dst_path = f"{tmp_path}/{file.name}"
        dbutils.fs.cp(file.path, dst_path)
      elif file.size == 0:
        copy_stray_files(file.path, tmp_path)
  
  import time
  tmp_path = f"{src_path}.temp-{time.time()}"
  try:
    dbutils.fs.rm(tmp_path, True)
    src_totals = spark.read.format("delta").load(src_path).count()
    copy_stray_files(src_path, tmp_path)
    tmp_totals = spark.read.parquet(tmp_path).count()
    return src_totals == tmp_totals
  finally:
    dbutils.fs.rm(tmp_path, True)

def validate_zordered_by_any(history, columns):
  passed = False
  
  if len(history) == 0:
    return False
  
  for column in columns:
    test_column = f"\"{column}\""
    data = list(filter(lambda r: (test_column in r), map(lambda r: r["zOrderBy"], history)))
    if len(data) > 0:
      passed = True
      
  return passed

def validate_num_buckets(table, expected_buckets):
  results = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter(FA.col("col_name") == "Num Buckets").select("data_type").collect()
  return str(expected_buckets) == results[0]["data_type"]

def validate_bucketed_by_any(table, expected_columns):
  results = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter(FA.col("col_name") == "Bucket Columns").select("data_type").collect()
  if not expected_columns or len(results) == 0: 
    return False
  
  for column in expected_columns:
    if f"`{column}`" in results[0]["data_type"]:
      return True

  return False

def validate_partitioned_by_any(history, columns):
  passed = False
  
  if len(history) == 0:
    return False
  
  for column in columns:
    test_column = f"\"{column}\""
    data = list(filter(lambda r: (test_column in r), map(lambda r: r["partitionBy"], history)))
    if len(data) > 0:
      passed = True
      
  return passed

def validate_table_exists(table_name):
  return len(list(__builtin__.filter(lambda t: t.name==table_name, spark.catalog.listTables(database_name)))) == 1

def validate_columns_exist(table_name, expected_columns):
  columns = spark.read.table(table_name).columns
  for column in expected_columns:
    if column not in columns:
      return False
      
  return True

def validate_broadcasted(data_frame):
  explained = explain_data_frame(data_frame)
  return "BroadcastExchange" in explained  

def validate_aqe_skew_join():
  aqe_enabled = str(spark.conf.get("spark.sql.adaptive.enabled")).lower() == "true"
  if not aqe_enabled: raise Exception("Adaptive Query Execution is not enabled.")
    
  sj_enabled = str(spark.conf.get("spark.sql.adaptive.skewJoin.enabled")).lower() == "true"
  if not sj_enabled: raise Exception("AQE's Skew Join is not enabled.")
    
  return aqe_enabled and sj_enabled

def validate_bloom_filter_indexes(table_path, expected_indexes, expected_fpp, expected_num_items):
  for field in spark.read.format("delta").load(table_path).schema:
    if field.name in expected_indexes:
    
      key = "delta.bloomFilter.fpp"
      if not key in field.metadata: 
        raise Exception(f"Missing index on {field.name}")
      elif field.metadata[key] != expected_fpp:
        raise Exception(f"""Unexpected FPP value on {field.name}'s bloom filter index""")
      
      # We have a bug where the specified numItems is not respected
      # key = "delta.bloomFilter.numItems"
      # if not key in field.metadata: 
      #   raise Exception(f"Missing index on {field.name}")
      # elif field.metadata[key] != expected_num_items:
      #   raise Exception(f"""Unexpected number of items for {field.name}'s bloom filter index""")
      
      key = "delta.bloomFilter.maxExpectedFpp"
      if not key in field.metadata: 
        raise Exception(f"Missing index on {field.name}")
      elif field.metadata[key] != 1:
        raise Exception(f"""Unexpected Maximum FPP value on {field.name}'s bloom filter index""")
      
      key = "delta.bloomFilter.enabled"
      if not key in field.metadata: 
        raise Exception(f"Missing index on {field.name}")
      elif not field.metadata[key]:
        raise Exception(f"""Boom filters are not enabled for the index {field.name}""")
      
    else:
      if "delta.bloomFilter.fpp" in field.metadata: 
        raise Exception(f"Unexpected index on {field.name}")
      if "delta.bloomFilter.numItems" in field.metadata: 
        raise Exception(f"Unexpected index on {field.name}")
      if "delta.bloomFilter.maxExpectedFpp" in field.metadata: 
        raise Exception(f"Unexpected index on {field.name}")
      if "delta.bloomFilter.enabled" in field.metadata: 
        raise Exception(f"Unexpected index on {field.name}")

  return True


None # Suppress output

# COMMAND ----------

class DatabricksAcademyLogger:

  def logSuite(self, suiteName, registrationId, suite):
    self.logEvent(f"Test-{suiteName}.suite", f"""{{
      "registrationId": "{registrationId}", 
      "testId": "Suite-{suiteName}", 
      "description": "Suite level results",
      "status": "{"passed" if suite.passed else "failed"}",
      "actPoints": "{suite.score}", 
      "maxPoints": "{suite.maxScore}",
      "percentage": "{suite.percentage}"
    }}""")

  def logAggregatedResults(self, lessonName, registrationId, aggregator):
    self.logEvent(f"Lesson.final", f"""{{
      "registrationId": "{registrationId}", 
      "testId": "Aggregated-{lessonName}", 
      "description": "Aggregated results for lesson",
      "status": "{"passed" if aggregator.passed else "failed"}",
      "actPoints": "{aggregator.score}", 
      "maxPoints":   "{aggregator.maxScore}",
      "percentage": "{aggregator.percentage}"
    }}""")
    
  def logEvent(self, eventId: str, message: str = None):
    import time
    import json
    import requests
    
    hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
    
    try:
      content = {
        "moduleName": "developer-advanced-capstone-v1",
        "lessonName": getLessonName(),
        "orgId":      getTag("orgId", "unknown"),
        "eventId":    eventId,
        "eventTime":  f"{int(__builtin__.round((time.time() * 1000)))}",
        "language":   getTag("notebookLanguage", "unknown"),
        "notebookId": getTag("notebookId", "unknown"),
        "sessionId":  getTag("sessionId", "unknown"),
        "message":    message
      }
      
      try:
        response = requests.post( 
            url=f"{hostname}/logger", 
            json=content,
            headers={
              "Accept": "application/json; charset=utf-8",
              "Content-Type": "application/json; charset=utf-8"
            })
        assert response.status_code == 200, f"Expected HTTP response code 200, found {response.status_code}"
      except requests.exceptions.RequestException as e:
        print("REQUEST ERROR ", e)
      
    except Exception as e:
      print("PYTHON ERROR", e)
      pass
    
daLogger = DatabricksAcademyLogger()

None # Suppress Output

# COMMAND ----------

# These imports are OK to provide for students
import pyspark
from typing import Callable, Any, Iterable, List, Set, Tuple
import uuid


#############################################
# Test Suite classes
#############################################

# Test case
class TestCase(object):
  __slots__=('description', 'testFunction', 'id', 'uniqueId', 'dependsOn', 'escapeHTML', 'points', 'hint')
  def __init__(self,
               description:str,
               testFunction:Callable[[], Any],
               id:str=None,
               dependsOn:Iterable[str]=[],
               escapeHTML:bool=False,
               points:int=1,
               hint=None):
    
    self.id=id
    self.hint=hint
    self.points=points
    self.escapeHTML=escapeHTML
    self.description=description
    self.testFunction=testFunction
    self.dependsOn = dependsOn if type(dependsOn) == list else [dependsOn]

# Test result
class TestResult(object):
  __slots__ = ('test', 'skipped', 'passed', 'status', 'points', 'exception', 'message')
  def __init__(self, test, skipped = False):
    try:
      self.test = test
      self.skipped = skipped
      if skipped:
        self.status = "skipped"
        self.passed = False
        self.points = 0
      else:
        assert test.testFunction() != False, "Test returned false"
        self.status = "passed"
        self.passed = True
        self.points = self.test.points
      self.exception = None
      self.message = ""
    except AssertionError as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = ""
    except Exception as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = str(e)

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

  
testResultsStyle = """
<style>
  table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
  caption { text-align: left; padding: 5px }
  th, td { border: 1px solid #ddd; padding: 5px }
  th { background-color: #ddd }
  .passed { background-color: #97d897 }
  .failed { background-color: #e2716c }
  .skipped { background-color: #f9d275 }
  .results .points { display: none }
  .results .message { display: block; font-size:smaller; color:gray }
  .results .note { display: block; font-size:smaller; font-decoration:italics }
  .results .passed::before  { content: "Passed" }
  .results .failed::before  { content: "Failed" }
  .results .skipped::before { content: "Skipped" }
  .grade .passed  .message:empty::before { content:"Passed" }
  .grade .failed  .message:empty::before { content:"Failed" }
  .grade .skipped .message:empty::before { content:"Skipped" }
</style>
    """.strip()

# Test suite class
class TestSuite(object):
  def __init__(self) -> None:
    self.ids = set()
    self.testCases = list()

  @lazy_property
  def testResults(self) -> List[TestResult]:
    return self.runTests()
  
  def runTests(self) -> List[TestResult]:
    import re
    failedTests = set()
    testResults = list()
    
    for test in self.testCases:
      skip = any(testId in failedTests for testId in test.dependsOn)
      result = TestResult(test, skip)

      if (not result.passed and test.id != None):
        failedTests.add(test.id)

      if result.test.id: eventId = "Test-"+result.test.id 
      elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
      else: eventId = "Test-"+str(uuid.uuid1())

      description = result.test.description.replace("\"", "'")
        
      message = f"""{{
        "registrationId": "{registration_id}", 
        "testId": "{eventId}", 
        "description": "{description}", 
        "status": "{result.status}", 
        "actPoints": "{result.points}", 
        "maxPoints": "{result.test.points}",
        "percentage": "{TestResultsAggregator.percentage}"
      }}"""

      daLogger.logEvent(eventId, message)

      testResults.append(result)
      TestResultsAggregator.update(result)
    
    return testResults

  def _display(self, cssClass:str="results") -> None:
    from html import escape
    lines = []
    lines.append(testResultsStyle)
    lines.append("<table class='"+cssClass+"'>")
    lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
    
    for result in self.testResults:
      descriptionHTML = escape(str(result.test.description)) if (result.test.escapeHTML) else str(result.test.description)
      lines.append(f"<tr>")
      lines.append(f"  <td class='points'>{str(result.points)}</td>")
      lines.append(f"  <td class='test'>")
      lines.append(f"    {descriptionHTML}")
                   
      if result.status == "failed" and result.test.hint:
        lines.append(f"  <div class='note'>Hint: {escape(str(result.test.hint))}</div>")
        
      if result.message:
        lines.append(f"    <hr/>")
        lines.append(f"    <div class='message'>{escape(str(result.message))}</div>")

      lines.append(f"  </td>")
      lines.append(f"  <td class='result {result.status}'></td>")
      lines.append(f"</tr>")
      
    lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
    lines.append("</table>")
    html = "\n".join(lines)
    displayHTML(html)
  
  def displayResults(self) -> None:
    self._display("results")
  
  def grade(self) -> int:
    self._display("grade")
    return self.score
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)

  @lazy_property
  def passed(self) -> bool:
    return self.percentage == 100

  def lastTestId(self) -> bool:
    return "-n/a-" if len(self.testCases) == 0 else self.testCases[-1].id

  def addTest(self, testCase: TestCase):
    if not testCase.id: raise ValueError("The test cases' id must be specified")
    if testCase.id in self.ids: raise ValueError(f"Duplicate test case id: {testCase.id}")
    self.testCases.append(testCase)
    self.ids.add(testCase.id)
    return self
  
  def test(self, id:str, description:str, testFunction:Callable[[], Any], points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False, hint=None):
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points, hint=hint)
    return self.addTest(testCase)
  
  def testEquals(self, id:str, description:str, valueA, valueB, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False, hint=None):
    testFunction = lambda: valueA == valueB
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points, hint=hint)
    return self.addTest(testCase)
  
  def failPreReq(self, id:str, e:Exception, dependsOn:Iterable[str]=[]):
    self.fail(id=id, points=1, dependsOn=dependsOn, escapeHTML=False,
              description=f"""<div>Execute prerequisites.</div><div style='max-width: 1024px; overflow-x:auto'>{e}</div>""")
    
  def fail(self, id:str, description:str, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testCase = TestCase(id=id, description=description, testFunction=lambda: False, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
    
  def testFloats(self, id:str, description:str, valueA, valueB, tolerance=0.01, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareFloats(valueA, valueB, tolerance)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  def testRows(self, id:str, description:str, rowA: pyspark.sql.Row, rowB: pyspark.sql.Row, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareRows(rowA, rowB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testDataFrames(self, id:str, description:str, dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareDataFrames(dfA, dfB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testContains(self, id:str, description:str, listOfValues, value, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: value in listOfValues
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  
class __TestResultsAggregator(object):
  testResults = dict()
  
  def update(self, result:TestResult):
    self.testResults[result.test.id] = result
    return result
  
  @property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults.values()))
  
  @property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults.values()))

  @property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)
  
  @property
  def passed(self) -> bool:
    return self.percentage == 100

  def displayResults(self):
    displayHTML(testResultsStyle + f"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.maxScore-self.score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
    </table>
    """)
# Lazy-man's singleton
TestResultsAggregator = __TestResultsAggregator()  

None # Suppress Output

# COMMAND ----------

import re

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", False)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", False)

spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", False)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", False)

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = getTag("user").lower()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

# The course_name is used to identify the dataset in dbacademy/courseware
# and as part of the user's globally unique working directory
course_name = "developer-advanced-capstone"
clean_course_name = re.sub("[^a-zA-Z0-9]", "_", course_name).lower() 

# The name of the current "lesson" or notebook
lesson_name = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]
clean_lesson_name = re.sub("[^a-zA-Z0-9]", "_", lesson_name).lower() # sanitize the lesson name for use in directory and database names

database_name = f"""dbacademy_{clean_username}_{clean_course_name}_{clean_lesson_name}"""
for i in range(10): database_name = database_name.replace("__", "_")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

# The path to our user's working directory. This combines both the
# username and course_name to create a "globally unique" folder.
working_dir = f"dbfs:/user/{clean_username}/dbacademy/{clean_course_name}"
  
def path_exists(path):
  try:
    return len(dbutils.fs.ls(path)) >= 0
  except Exception:
    return False
  
def install_exercise_datasets(exercise, target_path, min_time, max_time, reinstall):
  # source_dir = f"dbfs:/mnt/work-jacob/{course_name}/{exercise}"
  source_dir = f"wasbs://courseware@dbacademy.blob.core.windows.net/{course_name}/v01/{exercise}"
  
  print(f"The source directory for this dataset is\n{source_dir}/\n")
  existing = path_exists(target_path)
  
  if not reinstall and existing:
    print(f"Skipping install of existing dataset to\n{target_path}")
    return 

  # Remove old versions of the previously installed datasets
  if existing:
    print(f"Removing previously installed datasets from\n{target_path}")
    dbutils.fs.rm(target_path, True)
    
  print(f"""Installing the datasets to {target_path}""")
  
  print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
      region that your workspace is in, this operation can take as little as {min_time} and 
      upwards to {max_time}, but this is a one-time operation.""")

  dbutils.fs.cp(source_dir, target_path, True)
  print(f"""\nThe install of the datasets completed successfully.""")
