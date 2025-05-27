from pyspark.sql.functions import lit

# ✅ Add Column
df = df.withColumn("new_column_name", lit("Value"))  # You can also use a column expression instead of lit()

# ✅ Rename Column
df = df.withColumnRenamed("Old_name", "New_name")

# ✅ Drop Column
df = df.drop("Column_name")


from pyspark.sql.functions import lit, concat, col

# ✅ Add new columns: bonus and Name
empdf = empdf \
    .withColumn("bonus", col("salary") * 0.1) \
    .withColumn("Name", concat(col("first_name"), lit(" "), col("last_name")))

# ✅ Rename columns: name → full_name, doj → date_of_join
empdf = empdf \
    .withColumnRenamed("name", "full_name") \
    .withColumnRenamed("doj", "date_of_join")

# ✅ Drop columns: name and bonus
empdf = empdf.drop("name").drop("bonus")

# ✅ Show final result
empdf.show()
