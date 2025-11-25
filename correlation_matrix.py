import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt 
''' function for correlation matrix '''
def correlation_matrix(dataframe:pd.DataFrame,dataset_name:str = '',method:str = 'pearson',strong_only:bool=True,target_variable:str = None,print_index:bool=False) -> None:
    '''
    FUNCTION : 
        - Perform Correlation Matrix with heatmap, and optional target variable
        - ARGS:
            - dataframe     :   pandas Dataframe as input
            - method        :   type of correlation method to use (pearson,spearman,kendall):
                - pearson   : Assumes linear relationships between variables in a normally distributed dataset.
                    - Using covariance and standard deviations of orginal data.Sensitive to outliers. 
                    - (r = sigma(xi - mean(x) * (y - mean(y) / sqrt(sigma(squared((xi - mean(x))) * sigma(squared(yi - mean(y))))))))
                    - OR: - numerator = np.sum((x - mean_x) * (y - mean_y))- denominator_x = np.sqrt(np.sum((x - mean_x) ** 2)), denominator_y = np.sqrt(np.sum((y - mean_y) ** 2)) - pearson_corr = numerator / (denominator_x * denominator_y

                - spearman  : Assumes monotonic relationships (2 things move in the same direction) but not strictly linear. 
                    - Good for outliers, using ranks of data points instead of values. 
                    - (p = 1-(6*sigma(dsquaredi) / n(nsquared - 1)))

                - kendall   : Kendall Tau Assumes nothing about the data (non-parametric). 
                    - Useful when non linear relationship is known. 
                    - Good with outliers and non-linearity. Measure agreement between pairs of data. 1= perfect agreement, -1=perfect disagreement, 0=no association.
                    - Good for categorical data
                    - r = (no of concordant pairs - no of discordant pairs / n(n-1)/2)

        - target_variable: OPTIONAL 
            - If entered, filters showing relationships of all variables against this one. Useful for business cases or large dataset reduction.
        - print_index: OPTIONAL 
            - Nothing needed if False. If true, needs a anchor vairable to house the list of columns found in the correlation matrix (useful for further analysis/filtering).
        - RETURNS:
            - Heatmap showing correllation matrix.
    '''
    #=======================================
    # Perform Correlation Matrix to build as function for later use 
    #=======================================
    #---------------------------------------
    # Perform the correlation - Pandas only as spark too verbose and avoid is you can
    #---------------------------------------
    method_choices = ['pearson','spearman','kendall']
    if method not in method_choices:
        raise ValueError(f'ERROR: method of {method} not in {method_choices}')

    print(f'INFO: Correlation method ({method}) will be applied')
    correlation_matrix= dataframe.corr(method = method, numeric_only = True)

    #---------------------------------------
    # Filter out weak / non-relationships : Too weak or could be noise/unuseful
    #---------------------------------------
    if strong_only ==True:
        print('INFO: As strong_only=True,Removing correlation values less than or equal to abs(0.2)+-.')
        correlation_matrix = correlation_matrix[correlation_matrix.abs() >= 0.2]
    #---------------------------------------
    # Check for target Variable presence - to determine layout 
    #---------------------------------------
    dataset_name = dataset_name.replace('_',' ')
    target_variable = target_variable
    target_variable_name = ''

    if target_variable is not None:
        target_variable_name = target_variable.replace("_"," ").upper()
        print(f"INFO: Filtering correlation matrix to only show relationships related to {target_variable} across dataset {dataset_name}.")
        #---------------------------------------
        # Drop target reference of itself, as will not be useful anyway
        #---------------------------------------
        correlation_matrix_target = correlation_matrix[target_variable].drop(index = target_variable)
        correlation_matrix_target = correlation_matrix_target.dropna(how='all')
        correlation_matrix_target = correlation_matrix_target.to_frame()
        #---------------------------------------
        # Display Heatmap of Results 
        #---------------------------------------
        plt.figure(figsize=(10,8))
        sns.heatmap(data = correlation_matrix_target,annot=True,cmap='coolwarm_r',center=0, vmin=-1,vmax=1)
        plt.title(f'Correlation Matrix ({method}) of relationships with {target_variable_name} across dataset {dataset_name}.')
        plt.xlabel('Target Variable')
        plt.show()
        #---------------------------------------
        # OPTIONAL - to save down index for further analysis 
        #---------------------------------------
        if print_index == True:
            index = correlation_matrix_target.index.tolist()
            index.append(target_variable)
            return index

    else: 
        print('INFO: Showing all pairings as no target_variable declared.')
        plt.figure(figsize=(10,8))
        sns.heatmap(data = correlation_matrix,annot=True,cmap='coolwarm_r',center=0, vmin=-1,vmax=1)
        plt.title(f'Correlation Matrix ({method}) across dataset {dataset_name}.')
        plt.show()
        #---------------------------------------
        # OPTIONAL - to save down index for further analysis 
        #---------------------------------------
        if print_index == True:
            index = correlation_matrix_target.index.tolist()
            index.append(target_variable)
            return index
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del method_choices,correlation_matrix,dataset_name,target_variable,target_variable_name,index
    gc.collect()
