#needs pandas, seaborn , matplotlib
print(''' function for quick and dirty ditrbutions and boxplots ''')
#=======================================
# Function:create histograms and boxplots for specified columns
#=======================================
def plot_histograms_boxplots(dataframe:pd.DataFrame,columns:list, dataset_name:str = None)-> None:
    fig, axes = plt.subplots(len(columns), 2, figsize=(12, 4 * len(columns)))
    for i, col in enumerate(columns):
        # Histogram
        sns.histplot(dataframe[col], kde=True, ax=axes[i, 0],color='#008531')
        axes[i, 0].set_title(f'Histogram of {col} in dataset {dataset_name}')
        axes[i,0].tick_params(axis='x',rotation=45)
        # Boxplot
        sns.boxplot(x=dataframe[col], ax=axes[i, 1],color='#008531')
        axes[i, 1].set_title(f'Boxplot of {col} in dataset {dataset_name}')
        axes[i,1].tick_params(axis='x',rotation=45)
    plt.tight_layout()
