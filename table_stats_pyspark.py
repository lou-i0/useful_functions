''' function for dataframe stats '''
# NEEDS PYSPARK
# import gc as gc 
#=======================================
#FUNCTION : to report table stats and meta data 
#=======================================
def table_stats(dataframe:DataFrame, dataframe_name:str) -> None:
    '''
    Function aim is to record record and column counts, and some addiitonal metadata stats
    - ARGS:
        - dataframe     :   Spark Dataframe as the input
        - dataframe_name:   Str to determine name of dataset
    - RETURNS:
        - Number of records and columns found in dataframe
        - Descriptive statistics of dataframe
        - Top 3 records of Dataset
    - WHY:
        - See at a glance the structure of your dataset without extra work.
    '''
    print(f'The {dataframe_name} Spark DataFrame has: \n{dataframe.count()} Records, \n{len(dataframe.columns)} Columns')
    display(dataframe.describe())
    display(dataframe.limit(3))
    gc.collect()
    return None
