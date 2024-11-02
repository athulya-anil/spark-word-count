import importlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import assignment3.data_cleaner as dc

# Reload the module to get the latest changes
importlib.reload(dc)

# Initialize Spark session
spark = SparkSession.builder.appName("WordFrequency").getOrCreate()

# Read the file into a DataFrame
hamlet_df = spark.read.text("hamlet.txt").cache()

# Clean the data, remove special characters, filter the empty strings and convert to lowercase
cleaned_data = dc.clean_dataset(hamlet_df)

# Group by word and count occurrences
word_count = cleaned_data.groupBy('word').count()

# Sort the words by frequency in descending order and get the top 20
top_words = word_count.orderBy(desc('count')).limit(20)

# Show the top 20 most frequent words with their counts
top_words.show(truncate=False)

# Stop the Spark session
spark.stop()