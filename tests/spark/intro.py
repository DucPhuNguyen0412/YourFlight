from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer, VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName('Practice').getOrCreate()
df_pyspark = spark.read.csv('test.csv')
# spark.read.option('header', 'true').csv('test.csv', inferSchema=True).show()

# df_pyspark.printSchema()

# type(df_pyspark.select('_c0'))

# df_pyspark.select(['_c0','_c2']).show()
# df_pyspark['_c0']

# df_pyspark.describe().show()

# Adding columns
# df_pyspark.withColumn()

# Dropping the column
# df_pyspark.drop()

# Rename column
# df_pyspark.withColumnRenamed('price', 'model').show()
# df_pyspark.withColumnRenamed('_c1', 'price').show()
# df_pyspark.withColumnRenamed('_c2', 'rating').show()
# df_pyspark.withColumnRenamed('_c3', 'reviews').show()
# df_pyspark.withColumnRenamed('_c4', 'title').show()

# Drop null
# df_pyspark.na.drop().show()

# Drop subset 
# df_pyspark.na.drop(how='any', subset=['_c0', '_c3']).show()

# Filling missing value
# df_pyspark.na.fill('Missing Value').show()

# imputer = Imputer (
#     inputCol=['_c1', '_c3'],
#     outputCol=["{}_imputed".format(c) for c in ['_c1', '_c3']]
# ).setStrategy("mean") # median, ....

# Filter Operations
# df_pyspark.filter('_c3<=1000').show()

# df_pyspark.filter(~(df_pyspark['_c3']<=10000)).show()

# df_pyspark.filter('_c3<=1000').select(["_c1", "_c2"]).show()

# df_pyspark.filter((df_pyspark['_c3']<=10000)
#                     | (df_pyspark['_c3']>=100)).show()

# df_pyspark.filter((df_pyspark['_c3']<=10000)
#                     & (df_pyspark['_c3']>=100)).show()

# Aggregate 
# Group by
# df_pyspark.groupBy('_c0').sum().show()

# Group by Department 
# df_pyspark.groupBy('_c1').sum().show()

# df_pyspark.groupBy('_c1').mean().show()

# df_pyspark.groupBy('_c1').count().show()

# df_pyspark.agg({'Price':'_c1'}).show()

# featureAssembler = VectorAssembler(inputCols=["_c2", "_c3"], outputCol=["Independent Features"])
# output = featureAssembler.transform(df_pyspark)
# output.show()

# finalized_data = output.select("Independent Features", "_c3")
# finalized_data.show()

# df_pyspark,test_data=finalized_data.randomSplit([0.8,0.2])
# regressor=LinearRegression(featuresCol="Independent Features", labelCol="_c3")
# regressor=regressor.fit(df_pyspark)

# regressor.coefficients
# regressor.intercept
# pred_result=regressor.evaluate(df_pyspark)
# pred_result.predictions.show()

# indexer=StringIndexer(inputCol="_c2", outputCol="model_indexed")
# df_train=indexer.fit(df_pyspark).transform(df_pyspark)
# df_train.show()

# df_vector=VectorAssembler(inputCols=["_c3","model_indexed"], outputCol=["Independent Features"])
# output=df_vector.transform(df_train)

# output.select("Independent Feature").show()

# output.show()
# finalized_data=output.select("Independent Feature", "_c3")
# df_pyspark,test_data=finalized_data.randomSplit([0.8,0.2])
# regressor=LinearRegression(featuresCol="Independent Features", labelCol="_c3")
# regressor=regressor.fit(df_pyspark)

# regressor.coefficients
# regressor.intercept
# pred_result=regressor.evaluate(df_pyspark)
# pred_result.predictions.show()