from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Filter Examples").getOrCreate()

# Sample employee data
employee_data = [
    (10, "Raj Kumar", "1999", "100", "M", 2000),
    (20, "Rahul Rajan", "2002", "200", "F", 8000),
    (30, "Raghav", "2010", "100", None, 6000),
    (40, "Raja Singh", "2004", "100", "M", 7000),
    (50, "Rama Krish", "2008", "400", "M", 1000),
    (60, "Rasul", "2014", "500", "M", 5000),
    (70, "Kumar Chand", "2004", "600", "M", 5000)
]

# Define schema
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]

# Create DataFrame
df = spark.createDataFrame(data=employee_data, schema=employee_schema)

# 1. Not equal to 50
df.filter(df.salary != 50).show()

# 2. AND condition
df.filter((col('salary') > 2000) & (col('employee_dept_id') > '100')).show()

# 3. OR condition
df.filter((col('salary') > 7000) | (col('employee_dept_id') > '500')).show()

# 4. isNull
df.filter(df.gender.isNull()).show()

# 5. isNotNull
df.filter(df.gender.isNotNull()).show()

# 6. LIKE
df.filter(df.name.like('%Raj%')).show()

# 7. IS IN
df.filter(df.name.isin('Raghav', 'Rasul')).show()

# 8. CONTAINS
df.filter(df.name.contains('Singh')).show()

# 9. STARTSWITH
df.filter(df.name.startswith('Ra')).show()

# 10. ENDSWITH
df.filter(df.name.endswith('sh')).show()
