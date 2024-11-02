from pyspark.sql import DataFrame
from pyspark.sql.functions import split, lower, regexp_replace, explode

def clean_dataset(df: DataFrame, should_split_explode: bool = True):
    """
    This function will clean the dataset by removing the rows with missing values.
    :param df: The input dataframe
    :param should_split_explode: Whether to explode the words into rows
                            Set to false for WordPairs.py
    :return: The cleaned dataframe
    """

    if not should_split_explode:
        df_lower = df.select(lower(df.value).alias('word'))
        df_cleaned = df_lower.select(regexp_replace(df_lower.word, '[^a-zA-Z0-9 ]', '').alias('word'))
        df_filtered = df_cleaned.filter(df_cleaned.word != '')
        return df_filtered

    # Convert everything to lower case and replace special characters
    # Explode the words into rows because regex_replace will accept strings
    df_lower_split = df.select(explode(split(lower(df.value), ' ')).alias('word'))

    # Filter out empty strings
    filtered_df = df_lower_split.filter(df_lower_split.word != '').alias('word')

    # Note to self: Regex will match anything that is not an alphabet or a digit or a whitespace and replace it with an empty string
    # Link to my regex testing => https://regex101.com/r/CmGc6G/1
    df_cleaned = filtered_df.withColumn('word',
        regexp_replace(filtered_df.word, '[^a-zA-Z0-9]', '').alias('word'))

    # Explode the words into rows and return
    return df_cleaned
