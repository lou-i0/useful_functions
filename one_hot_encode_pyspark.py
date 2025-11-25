''' function for one ot encoding in pypsark  '''
# needs 
#import gc as gc 
#from pyspark.ml.feature import StringIndexer    # Converting Categories into numeric form.
#from pyspark.ml.feature import OneHotEncoder    # Coverting numeric values into vector form.
#from pyspark.ml.feature import VectorAssembler  # Converting columns to vectors?
#from pyspark.sql import DataFrame               # Pyspark Dataframe reference to explicit type setting
#=======================================
#FUNCTION : Perform One Hot Encoding on Spark DataFrame
#=======================================
def one_hot_encode(spark_df:DataFrame,input_col:str) -> DataFrame:
    '''
    - FUNCTION:
        - To perform one hot encoding on categorical column, into a numerical vector form for Machine Learning. 
        - Think as a Spark version of pd.get_dummies(). Instead of separated into its own columns, its contained within one column as a vector.
    - ARGS:
        - spark_df    : Spark Dataframe where column is to be encoded
        - input_col   : Name of original column in dataframe
        - output_col  : what transformed columns should be named as
    - RETURNS:
        - Spark DataFrame with Transformed columns applied.
    - WHY:
        - vectors can be used in Spark ML Models going forward, where categories cannot.
    '''
    #---------------------------------------
    # Apply String index to convert Sector/Area into a numeric value # 0's is the most frequent label (GOOD TO KNOW "!")
    #---------------------------------------
    indexer = StringIndexer(inputCol=input_col, outputCol=f"{input_col}_N")
    #---------------------------------------
    # Apply one Hot Encoder to combine numeric values from previous stap into a vector with one value representing the original category in nueric form across available cataegories in dataset.
    #---------------------------------------
    ohe = OneHotEncoder(inputCol=f"{input_col}_N",outputCol=f"{input_col}_ohe")

    #---------------------------------------
    # Create Pipeline to apply steps 
    #---------------------------------------
    ohe_pipe            = Pipeline(stages = [indexer,ohe])
    ohe_model           = ohe_pipe.fit(spark_df)
    spark_df_ohe  = ohe_model.transform(spark_df)
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del indexer, ohe, ohe_pipe,ohe_model
    gc.collect()

    return spark_df_ohe
