from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, regexp_replace

# Create a spark session with the class name
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the input file
hamlet_df = spark.read.text("hamlet.txt").cache()
# hamlet_df.show()

## Print dimensions of the dataframe
# print((hamlet_df.count(), len(hamlet_df.columns)))

# Split the lines into words (but this will come as a PySpark Column)
split_df = hamlet_df.select(split(hamlet_df.value, ' ').alias('words'))

# Explode the words into rows
explode_df = split_df.select(explode(split_df.words).alias('words'))

# Now we are ready to count the words (which basically are the rows)
word_count = explode_df.count()

# Print the word count
print("Word count: ", word_count)

new_df = hamlet_df.select(explode(split(lower(hamlet_df.value), ' ')).alias('word'))
cleaned_df = new_df.select(regexp_replace(new_df.word, '[^a-zA-Z0-9]', '').alias('word'))
filtered_df = cleaned_df.filter(cleaned_df.word != '')

print("Word count after cleaning and removing duplicates: ", filtered_df.count())

# Stop the spark session
spark.stop()