import pandas as pd
import gc

def unidentified_like_check_pandas(dataframe: pd.DataFrame, dataframe_name: str = '') -> None:
    """
    - FUNCTION:
        - Take record count of Pandas DataFrame
        - Show columns with values containing 'UNIDENTIFIED'
        - Show % of dataset with affected records
        - Show count per column
        - Present DataFrame of affected records
    - ARGS:
        - dataframe: Pandas DataFrame as input
    - RETURNS:
        Prints summary and displays subset with 'UNIDENTIFIED' values
    """
    print('#=======================================')
    print(f'☑️🔍INFO: Performing LIKE %%UNIDENTIFIED%% Check on Dataset: {dataframe_name}')
    print('#=======================================')

    #---------------------------------------
    # Total records
    #---------------------------------------
    total_records = len(dataframe)

    #---------------------------------------
    # Count of "UNIDENTIFIED" (LIKE match) per column
    #---------------------------------------
    unidentified_summary = {}
    for col in dataframe.columns:
        if dataframe[col].dtype == "object":  # only check string columns
            count = dataframe[col].str.contains("UNIDENTIFIED", case=False, na=False).sum()
            unidentified_summary[col] = count

    #---------------------------------------
    # Records with any "UNIDENTIFIED" substring
    #---------------------------------------
    mask = dataframe.apply(
        lambda col: col.astype(str).str.contains("UNIDENTIFIED", case=False, na=False)
        if col.dtype == "object" else False
    ).any(axis=1)
    df_unidentified = dataframe[mask]
    unidentified_record_count = len(df_unidentified)

    print(f'📊 Total Records from dataset: {total_records}')
    print(f'⚠️ Records with UNIDENTIFIED fields: {unidentified_record_count}')
    print(f'📉 Percentage of UNIDENTIFIED records in dataset: {round(unidentified_record_count / total_records * 100, 2)}%')

    #---------------------------------------
    # Show number of records with UNIDENTIFIED per column
    #---------------------------------------
    print('\n🧾 UNIDENTIFIED counts per column:')
    for col_name, count in unidentified_summary.items():
        if count > 0:
            print(f' - {col_name}: {count} matches ({round(count / total_records * 100, 2)}%)')

    #---------------------------------------
    # Show all records with UNIDENTIFIED values
    #---------------------------------------
    print('\n🧾 Showing all records with UNIDENTIFIED values:')
    display(df_unidentified)
    print('#=======================================')
    print(f'✅🏁INFO: LIKE %%UNIDENTIFIED%% Check Completed on Dataset: {dataframe_name}')
    print('#=======================================')

    #---------------------------------------
    # Clean up
    #---------------------------------------
    del total_records, unidentified_summary, unidentified_record_count
    gc.collect()

    # Example usage:
    # unidentified_like_check_pandas(df.toPandas(), "MyDataset")
