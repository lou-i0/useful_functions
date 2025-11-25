#uses pyspark, pandas, import scipy.stats                     as stats # Hypothesis testing

print(''' function for chi-square test ''')
#=======================================
# Chi-Square test
#=======================================
def chi_square_test(dataframe:DataFrame,category_column:str,target_column:str,p_threshold:int=0.05,agg:str='count') -> pd.DataFrame:
    '''
    - Function: 
        - Perform Chi-Sqaure Test and returns pandas dataframes for calculated values
    - ARGS:
        - dataframe: Spark DataFrame as input (converted to Pandas).
        - category_column: column to measure association against. 
        - target_column: column to measure fluctuations in category.
        - p_threshold: p_value to pass or fail the alternative hypothesis (default 0.05). 
    - RETURNS:
        - observed_df = Crosstab count of number of targets in each category.
        - expected_df = Shows what the test expected the counts to be. 
        - residual_df = measure difference observed - expected.
    - REFERENCE:
        - X² = Σ[(Observed - Expected)² / Expected]
        - H₀: Category is independent of Target
        - H₁: Target rates vary by Category
    '''
    #---------------------------------------
    # Config for test 
    #---------------------------------------
    p_value_threshold = p_threshold               # Is less than this, confirm that Category Dependant on Target
    #---------------------------------------
    # Convert to pandas dataframe - may be changed in future
    #---------------------------------------
    dataset = dataframe.toPandas()
    #---------------------------------------
    # use heat pivot function created earlier to get the crosstab table 
    #---------------------------------------
    test_data = heat_pivot(
                            dataframe = dataset, dataset_name='Chi-Square Test Data'
                            ,index = category_column,columns=target_column,values = target_column
                            ,mode='table',aggregate=agg
                        )
    test_data_columns = test_data.columns.tolist()

    # return test_data
    #---------------------------------------
    # Perform Chi-Square Test
    #---------------------------------------
    chi2, p_value, dof, expected = stats.chi2_contingency(test_data)

    print(f"Chi-square test for {category_column} vs {target_column}") # to change later
    print("Chi-Sqaured Statistic:", chi2)
    print("Degrees of Freedom:", dof)
    print(f"P-value:", format(p_value,'.10f'))

    if p_value < p_value_threshold:
        print(f"✅ Null Hypothesis Rejected - {target_column} rates vary by {category_column} (p value < {p_value_threshold})")
    else:
        print(f"❌ Failed to Reject Null Hypothesis - {target_column} rates do not significantly vary by {category_column}")

    #---------------------------------------
    # Show observed and expected values from the initial transformed data
    #---------------------------------------
    observed    = test_data.values
    observed_df =pd.DataFrame(observed,index = test_data.index, columns = test_data.columns)#.rename(columns = {0:'Unregistered',1:'Registered'})
    #---------------------------------------
    # Measure difference between observed vs test expectations if no relationship assumed
    # Show who is off track generally towards registration
    #---------------------------------------
    residuals   = observed - expected
    expected_df = pd.DataFrame(expected,index = test_data.index, columns = test_data.columns)#.rename(columns = {0:'Unregistered',1:'Registered'})
    expected_safe = expected_df.replace(0, np.nan)
    residuals_df = pd.DataFrame(residuals,index = test_data.index, columns = test_data.columns)#.rename(columns = {0:'Unregistered',1:'Registered'})
    percent_residuals_df = (residuals_df / expected_safe) *100
    percent_residuals_df = percent_residuals_df.round(2)
    

    #---------------------------------------
    # In this Test, log registered sectors by observed, expected and the residuals
    # Show who is off track generally towards registration
    #---------------------------------------
    print('➕ More completed Than expected | ➖ Less completed than expected in residual values')

    summary_df = pd.concat([observed_df.add_prefix('Observed_')
                            ,expected_df.round(2).astype(int).add_prefix('Expected_')
                            ,residuals_df.round(2).astype(int).add_prefix('Residual_')
                            ,percent_residuals_df.add_prefix('PercentResidual_')]
                            , axis=1).reset_index()

    return summary_df
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del p_value_threshold,dataset,test_data,chi2, p_value, dof, expected,observed,observed_df,residuals,residuals_df,expected_df
    gc.collect() 
