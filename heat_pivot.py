import pandas as py 
import seaborn as sns 
import matplotlib.pyplot as plt
''' function for heat pivot '''
#=======================================
#FUNCTION : heatmap based on crosstab
#=======================================
def heat_pivot(dataframe: pd.DataFrame,dataset_name:str, index:str,columns:str,values:str,aggregate:str = 'sum', mode:str ='heatmap') -> None:
    '''
    FUNCTION: to provide pivot table on dataframe and params to produce the table or heatmap respectively
    - ARGS:
        - dataframe     : Pandas Dataframe
        - dataset_name  : Any str to name the dataset
        - index         : Column to use as rows to measure against
        - columns       : Column to use as differentiator
        - values        : Column values to aggregate
        - aggregate     : Type of agg to use, default is sum, but mean and others can be used
        - mode          : if 'heatmap' will show the plot, if 'table' , return the crosstab table
    - RETURNS
        - WHEN mode = 'heatmap'   THEN sns heatmap    (no anchor variable needed)
        - WHEN mode = 'table'     THEN crosstab table (anchor variable needed)
    '''
    try:
        mode_choices = ['heatmap','table']
        if mode not in mode_choices:
            raise ValueError(f'{mode} is not in {mode_choices}')
        #----------------------------------------
        # returns plot (no variable needed)
        #----------------------------------------
        if mode == 'heatmap' :
            #----------------------------------------
            # Provide label names for chart
            #----------------------------------------
            index_name      = index.upper()
            columns_name    =  columns.upper()
            values_name     = values.upper()
            aggregate_name  = aggregate.upper()
            dataset_name    = dataset_name.upper()
            
            #----------------------------------------
            # Create Crosstab (pivot Table) based on params
            #----------------------------------------
            crosstab_result = pd.crosstab(index   = dataframe[index]          # Rows
                                          ,columns= dataframe[columns]     # Columns
                                          ,values = dataframe[values]       # Optional: values to aggregate
                                          ,aggfunc= aggregate).fillna(0).astype(int)            # Aggregation function: 'sum', 'mean', 'count', etc.
            #----------------------------------------
            # Create Heatmap 
            #----------------------------------------
            plt.figsize=(25, 18)
            sns.heatmap(data = crosstab_result, annot=True, fmt='.0f' ,cmap = 'GnBu', linewidths=0.5, linecolor='grey'
                        ,cbar_kws={'label': f'{aggregate_name} of {values_name}'}
                        ,annot_kws={"size": 12, "weight": "bold", "color": "Black","clip_on": False})

            plt.title(f"Heatmap of {index_name} vs {columns_name}  by {values_name} ({aggregate_name}) for the {dataset_name} Dataset")
            plt.xlabel(columns_name,fontsize = 12, fontweight = 'bold')
            plt.ylabel(index_name, fontsize=12, fontweight = 'bold')
            plt.xticks(rotation=45,fontsize = 12)
            plt.yticks(rotation=0,fontsize=10)
            #plt.tight_layout()
            plt.show()
        #----------------------------------------
        # returns crosstab dataframe (ensure variable to pick it up)
        #----------------------------------------
        elif mode == 'table':
            crosstab_result = pd.crosstab(index=dataframe[index],columns=dataframe[columns],values=dataframe[values],aggfunc=aggregate).fillna(0).astype(int)
            return crosstab_result

    except Exception as e:
        print(f'Something went wrong: {e}')
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del mode_choices, index_name ,columns_name ,values_name ,aggregate_name ,dataset_name ,crosstab_result  
    gc.collect()
