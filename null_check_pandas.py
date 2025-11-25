import pandas as pd
import gc

def null_check_pandas(dataframe: pd.DataFrame,dataframe_name:str = '') -> None:
    """
    - FUNCTION:
        - Take record count of Pandas DataFrame
        - Show columns with NULL values and counts
        - Show % of dataset with NULL records
        - Show null count per column
        - Present DataFrame of affected records with NULLs
    - ARGS:
        - df: Pandas DataFrame as input
    - RETURNS:
        Prints summary and displays subset with nulls
    """
    print('#=======================================')
    print(f'☑️🔋INFO: Performing null Check on Dataset: {dataframe_name}')
    print('#=======================================')
    #---------------------------------------
    # Total records
    #---------------------------------------
    total_records = len(dataframe)

    #---------------------------------------
    # Null count per column
    #---------------------------------------
    null_summary = dataframe.isnull().sum()

    #---------------------------------------
    # Records with any nulls
    #---------------------------------------
    df_nulls = dataframe[dataframe.isnull().any(axis=1)]
    null_record_count = len(df_nulls)

    print(f'📊 Total Records from dataset: {total_records}')
    print(f'⚠️ Records with null fields: {null_record_count}')
    print(f'📉 Percentage of null records in dataset: {round(null_record_count / total_records * 100, 2)}%')

    #---------------------------------------
    # Show number of records with nulls per column
    #---------------------------------------
    print('\n🧾 Null counts per column:')
    for col_name, nulls in null_summary.items():
        if nulls > 0:
            print(f' - {col_name}: {nulls} nulls ({round(nulls / total_records * 100, 2)}%)')

    #---------------------------------------
    # Show all null records in datset
    #---------------------------------------
    print('\n🧾 Showing all null records in dataset:')
    display(df_nulls)
    print('#=======================================')
    print(f'✅🏁INFO: Null Check Completed on Dataset: {dataframe_name}')
    print('#=======================================')

    #---------------------------------------
    # Clean up
    #---------------------------------------
    del total_records, null_summary, null_record_count
    gc.collect()

    # null_check_pandas(df.toPandas())
