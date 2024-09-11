from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class SparkDataFrameSorter:
    @staticmethod
    def deterministic_sort(df: DataFrame) -> DataFrame:
        """
        This function deterministically sorts the dataframe by sorting by each column ascending.  Null values are sorted
        to the end

        :param df: DataFrame to sort
        :returns sorted dataframe
        """

        if df.isEmpty() or len(df.columns) == 0:
            return df

        # Get the list of columns
        columns = df.columns

        # Sort by all the columns in ascending order
        sorted_df: DataFrame = df.orderBy([col(c).asc_nulls_last() for c in columns])

        return sorted_df
