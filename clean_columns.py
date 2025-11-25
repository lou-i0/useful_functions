''' Function to clean up column names '''
#needs pandas
#=======================================
#FUNCTION : clean up column names
#=======================================
def clean_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame column names by replacing spaces with underscores
    and converting them to lowercase.
    - ARGS : 
        - pandas dataframe as input for cleaning 
    - RETURNS:
        - Cleaned column Pandas DataFrame
    - WHY:
        To have columns cleaned and avoids issues with using them in future. 
    """
    new_columns = []

    for col in dataframe.columns:
        clean_col = str(col).lower().strip()
        clean_col = clean_col.replace(" ","_")
        clean_col = re.sub(r'[^a-z0-9_]+','',clean_col)
        new_columns.append(clean_col)

    dataframe.columns = new_columns
    gc.collect()
    return dataframe
