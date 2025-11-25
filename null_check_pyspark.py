
''' function for null check analysis '''
# from pyspark.sql import DataFrame               # Pyspark Dataframe reference to explicit type setting
# from pyspark.sql import functions      as f     # pyspark SQL like Functions
# import gc as gc 
#from functools import reduce                    # Applies a function to a iterable to show a single value result collection of elements into a singular value
#=======================================
#FUNCTION : Checking for Null counts in Spark Dataframe
#=======================================
def null_check(dataframe:DataFrame,dataframe_name:str = '') -> None:
    '''
    - FUNCTION : 
        - Take record count of spark Dartaframe
        - Then, glean columns across a Spark Dataframe where NULL values are found and how many records
        - Shows % of dataset with NULL records
        - Shows null count per column
        - Present Spark Dataframe of the affect records with NULLS
    - ARGS:
        - spark_df: Spark Dataframe as input
    - RETURNS:
        As above. 
    '''
    print('#=======================================')
    print(f'☑️🔋INFO: Performing null Check on Dataset: {dataframe_name}')
    print('#=======================================')
    #---------------------------------------
    # Log total Records in dataset
    #---------------------------------------
    total_records = dataframe.count()
    #---------------------------------------
    #  Check Null count per column found to have nulls
    #---------------------------------------
    null_summary = dataframe.select([f.count(f.when(f.col(c).isNull(),c)).alias(c) for c in dataframe.columns]).collect()[0].asDict()
    #---------------------------------------
    # Show number of records with any nulls
    #---------------------------------------
    null_conditions = [f.col(c).isNull() for c in dataframe.columns]
    df_nulls = dataframe.filter(reduce(lambda x, y: x | y, null_conditions))
    df_null_count = df_nulls.count()

    print(f'📊Total Records from combined dataset: {total_records}')
    print(f'⚠️Records with null fields: {df_null_count}')
    print(f'📉Percentage of null records in dataset: {round(df_null_count/total_records*100,2)}%')
    #---------------------------------------
    # Show number of records with nulls per column
    #---------------------------------------
    print('\n🔢 Null counts per column:')
    for col_name, nulls in null_summary.items():
        if nulls > 0:
            print(f' - {col_name}: {nulls} nulls ({round(nulls / total_records * 100, 2)}%)')

    print('#=======================================')
    print(f'✅🏁INFO: Null Check Completed on Dataset: {dataframe_name}')
    print('#=======================================')
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del total_records, null_summary,null_conditions, df_null_count
    gc.collect()

    display(df_nulls)
