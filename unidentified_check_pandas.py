import pandas as pd
import gc

def unidentified_check_pandas(dataframe: pd.DataFrame, dataframe_name: str = '') -> None:
    """
    - FUNCTION:
        - Take record count of Pandas DataFrame
        - Show columns with 'UNINDENTIFIED' values and counts
        - Show % of dataset with 'UNINDENTIFIED' records
        - Show count per column
        - Present DataFrame of affected records
    - ARGS:
        - dataframe: Pandas DataFrame as input
    - RETURNS:
        Prints summary and displays subset with 'UNINDENTIFIED' values
    """
    print('#=======================================')
    print(f'☑️🔍INFO: Performing UNINDENTIFIED Check on Dataset: {dataframe_name}')
    print('#=======================================')

    #---------------------------------------
    # Total records
    #---------------------------------------
    total_records = len(dataframe)

    #---------------------------------------
    # Count of "UNINDENTIFIED" per column
    #---------------------------------------
    unidentified_summary = (dataframe == "UNINDENTIFIED").sum()

    #---------------------------------------
    # Records with any "UNINDENTIFIED"
    #---------------------------------------
    df_unidentified = dataframe[dataframe.eq("UNINDENTIFIED").any(axis=1)]
    unidentified_record_count = len(df_unidentified)

    print(f'📊 Total Records from dataset: {total_records}')
    print(f'⚠️ Records with UNINDENTIFIED fields: {unidentified_record_count}')
    print(f'📉 Percentage of UNINDENTIFIED records in dataset: {round(unidentified_record_count / total_records * 100, 2)}%')

    #---------------------------------------
    # Show number of records with UNINDENTIFIED per column
    #---------------------------------------
    print('\n🧾 UNINDENTIFIED counts per column:')
    for col_name, count in unidentified_summary.items():
        if count > 0:
            print(f' - {col_name}: {count} UNINDENTIFIED ({round(count / total_records * 100, 2)}%)')

    #---------------------------------------
    # Show all records with UNINDENTIFIED values
    #---------------------------------------
    print('\n🧾 Showing all records with UNINDENTIFIED values:')
    display(df_unidentified)
    print('#=======================================')
    print(f'✅🏁INFO: UNINDENTIFIED Check Completed on Dataset: {dataframe_name}')
    print('#=======================================')

    #---------------------------------------
    # Clean up
    #---------------------------------------
    del total_records, unidentified_summary, unidentified_record_count
    gc.collect()

    # Example usage:
    # unidentified_check_pandas(df.toPandas(), "MyDataset")
