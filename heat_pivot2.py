import seaborn as sns 
import matplotlib.pyplot as plt 
import pandas pd 
#=======================================
#FUNCTION : heatmap based on crosstab
#=======================================
def heat_pivot(dataframe: pd.DataFrame,dataset_name:str, index:str,columns:str,values:str,aggregate:str = 'sum') -> None:

    index_name      = index.upper()
    columns_name    =  columns.upper()
    values_name = values.upper()
    aggregate_name = aggregate.upper()
    dataset_name = dataset_name.upper()
    

    crosstab_result = pd.crosstab(
                                    index=dataframe[index]          # Rows
                                    ,columns=dataframe[columns]     # Columns
                                    ,values=dataframe[values]       # Optional: values to aggregate
                                    ,aggfunc=aggregate              # Aggregation function: 'sum', 'mean', 'count', etc.
                                 )

    plt.figsize=(25, 18)
    sns.heatmap(
                data = crosstab_result, annot=True, fmt='.0f' ,cmap = 'GnBu', linewidths=0.5, linecolor='grey'
                ,cbar_kws={'label': f'{aggregate_name} of {values_name}'}
                ,annot_kws={"size": 12, "weight": "bold", "color": "Black","clip_on": False}
                )

    plt.title(f"Heatmap of {index_name} vs {columns_name}  by {values_name} ({aggregate_name}) for the {dataset_name} Dataset")
    plt.xlabel(columns_name,fontsize = 12, fontweight = 'bold')
    plt.ylabel(index_name, fontsize=12, fontweight = 'bold')
    plt.xticks(rotation=45,fontsize = 12)
    plt.yticks(rotation=0,fontsize=10)
    #plt.tight_layout()
    plt.show()
