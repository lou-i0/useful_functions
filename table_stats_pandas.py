import pandas as pd
import gc

#=======================================
# FUNCTION : to report table stats and meta data (Pandas version)
#=======================================
def table_stats(df: pd.DataFrame, dataframe_name: str = '') -> None:
    """
    Function aim is to record record and column counts, and some additional metadata stats
    - ARGS:
        - df             : Pandas DataFrame as the input
        - dataframe_name : Str to determine name of dataset
    - RETURNS:
        - Number of records and columns found in dataframe
        - Descriptive statistics of dataframe
        - Top 3 records of Dataset
    - WHY:
        - See at a glance the structure of your dataset without extra work.
    """
    print('#=======================================')
    print(f'🔋☑️INFO: Performing Table Check on Dataset: {dataframe_name}')
    print('#=======================================')
    print(f'The {dataframe_name} DataFrame has: \n{len(df)} Records, \n{df.shape[1]} Columns')

    #-------------------------------------
    # Quick Stats
    #-------------------------------------
    print("\n📊 Descriptive statistics:")
    display(df.describe(include='all'))

    #-------------------------------------
    # Check if there are date columns
    #-------------------------------------
    print("\n📆 Date column information:")
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            min_val = df[col].min()
            max_val = df[col].max()
            print(f" - {col}: Earliest Date = {min_val}, Latest Date = {max_val}")

    #-------------------------------------
    # Show Schema (info)
    #-------------------------------------
    print(f"\n🏗️ Schema of {dataframe_name} Dataset:")
    df.info()

    #-------------------------------------
    # Show preview of dataset
    #-------------------------------------
    print(f"\n🔬 Sample 3 records below of {dataframe_name} Dataset:")
    display(df.head(3))

    print('#=======================================')
    print(f'✅🏁INFO: Table Check Completed on Dataset: {dataframe_name}')
    print('#=======================================')
    gc.collect()
    return None
