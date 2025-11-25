print(''' function for 2 sample t-test''')
#=======================================
# Apply 2 sample t-test on licence returns (split by filter columns and value)
#=======================================
def two_ttest(dataframe:pd.DataFrame, dtype:list,filter_column:str,filter_value,alternative:str = 'two-sided') -> None:
    '''
        - Function: Two sided t-test , showing result in dataframe 
        - ARGS:
            - dataframe: Pandas DataFrame as input
            - dtype: list of stypes to select for test, only number and/or bool accepted
            - filter_column: column to split dataset by
            - filter_value: value in filter column to split with
        - RETURNS
            - displays pandas dataframe of:
                - column picked up in dataframe
                - p-value
                - t-statistic
                - whether the null hytpothesis could be rejected per column tested
    '''
    filter_column   = filter_column
    filter_value    = filter_value
    dtype           = dtype

    #---------------------------------------
    # Only show numerical columns (area/sector fields not included)
    #---------------------------------------
    dtype_cols = dataframe.select_dtypes(include=dtype).columns.drop(filter_column)

    #---------------------------------------
    # Split dataset into subsets based on whether there are wrls registered (1) or not (0)
    #---------------------------------------
    dataframe_1 = dataframe[dataframe[filter_column] >= filter_value]
    dataframe_0 = dataframe[dataframe[filter_column] < filter_value]

    #---------------------------------------
    # Apply t-test between subsets per numerical column
    #---------------------------------------
    ttest_dataframe = pd.DataFrame()
    for col in dtype_cols:
        t_stat,p_val = stats.ttest_ind(a = dataframe_1[col],b = dataframe_0[col],equal_var = False,random_state = 27,alternative=alternative)
        if p_val < 0.05:
            #print(f'\n As P value of \033[1m{p_val:.5f}\033[0m, is less then 0.05, this indicates \033[1m{col}\033[0m from each group is statisically different from each other. \033[1mNull Hypothesis Rejected\033[0m. \n')
            Ha = pd.DataFrame([{'column':col,'p-value':round(p_val,5),'t-statistic':round(t_stat,5),'null_hypothesis_rejected':1}])
            ttest_dataframe = pd.concat([ttest_dataframe,Ha],ignore_index=True)
        else:
            #print(f'\n As P value of {p_val:.5f} is more than 0.05, this indicates \033[1m{col} fails to reject null hypothesis as groups are similar\033[0m. \n')
            H0 = pd.DataFrame([{'column':col,'p-value':round(p_val,5),'t-statistic':round(t_stat,5),'null_hypothesis_rejected':0}])
            ttest_dataframe = pd.concat([ttest_dataframe,H0],ignore_index=True)

    print('If p-value is less then 0.05, this indicates column from each group is statisically different from each other. Therefore, \033[1mNull Hypothesis Rejected\033[0m. \n')
    print('Otherwise, failed to reject Null hypothesis as values between groups are similar.')

    
    #---------------------------------------
    # Create Visual to so columns vs t-stat broken by null rejection.
    #---------------------------------------
    fig = px.bar(data_frame = ttest_dataframe,x = 'column',y='t-statistic', color = 't-statistic',color_continuous_scale=['#F46A25','#008531'])

    fig.update_layout( title           ='Columns between subsets by t-statistic'
                       ,plot_bgcolor   = 'white'
                       ,xaxis          =dict(title='Dataset Column'),yaxis=dict(title='T-Statistic')
                       ,legend=dict(orientation = 'h', xanchor = 'center',x = 0.5,y = 1.20,font=dict(size=12))
                       ,bargap=0.2
                       ,height = 1000)

    fig.update_layout(margin=dict(l=50, r=50, t=100, b=50))
    fig.show()
    #---------------------------------------
    # Present results back 
    #---------------------------------------
    return ttest_dataframe
    #---------------------------------------
    # Clean up
    #---------------------------------------
    del filter_column,filter_value,dtype,dtype_cols,dataframe_1,dataframe_0,ttest_dataframe,fig
    gc.collect()
