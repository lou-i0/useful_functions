print('''Function for k-means clustering algorithm''')
#=======================================
# Perform k-means clustering algorithm
#=======================================
#from pyspark.ml.clustering import KMeans        # K-means clustering 
#from pyspark.ml.evaluation import ClusteringEvaluator #clustering eval ( siloette score / elbow method)
#from pyspark.ml.feature import PCA             # Principal Component Analysis (Dimensionaly reduction)
#import plotly.express as px                     # Interactive Data Visulisations (better than matplotlib in my opnion)
#import plotly.graph_objects            as go    #  For more customised plotly charts
#import plotly.io      as pio                    # set up template
#from pyspark.sql import DataFrame               # Pyspark Dataframe reference to explicit type setting
#from sklearn.manifold import TSNE               # for non-linear dimensionality reduction to show data in 2 dimensions
def kmeans_clustering(dataframe:DataFrame,featurescol:str,test:str = 'Y',k:int = None) -> DataFrame:
    '''
    - FUNCTION: Perform K-Means Clustering onto a Spark Dataframe with Vectorised features
    - ARGS:
        - dataframe     : PySpark DataFrame of dataset with vectorised column of features (required to work).
        - featurescol   : Column in dataframe of vectorised colums of dataset ( need to use vector assembler, plus any categories one hot encoded beforehand)
        - test      : 
            - if 'Y': Will find the optimised k for clusters to use with elbow chart and Silhouette Score.
            - if 'N': Will process k-means model with selected K once test is done prior.
    - RETURNS:
        - if test = 'Y': Produces Elbow method chart and silhouette scores. to evidence which number k to use. 
        - if test = 'N': Run K-mean model against dataset, and returns dataset with kmeans predictions
            - will also plot pca and t-sne to visualise clusters formed on the dimensionally redcued data.
    '''
    test_result = test
    if test_result == 'Y':
        print('===============================================================')
        print('INFO: As test = Y, Performing test to find optimal k for model and dataset.')
        print('===============================================================\n')

        #-----------------------------------------------------
        # Evaulate right k for k-means model buy running iterations
        #-----------------------------------------------------
        silhouette_score=[]
        evaluator = ClusteringEvaluator(predictionCol   ='prediction'
                                        ,featuresCol    = featurescol
                                        ,metricName     ='silhouette'
                                        ,distanceMeasure='squaredEuclidean')

        for i in range(2,10):
            kmeans=KMeans(featuresCol=featurescol, k=i)
            model=kmeans.fit(dataframe)
            predictions=model.transform(dataframe)
            score=evaluator.evaluate(predictions)
            silhouette_score.append(score)
            print('Silhouette Score for k =',i,'is',score)
        

        #-----------------------------------------------------
        # Chart plot for elbow method to find the right k with silhoutte score
        #-----------------------------------------------------
        fig = px.line(y = silhouette_score,x=range(2,10))
        fig.update_layout(
                        title           ='finding best k cluster based on Silhouette Score/Elbow method'
                        ,plot_bgcolor   = 'white'
                        ,xaxis          =dict(title='K'),yaxis=dict(title='Silhouette Score')
                        ,legend=dict(orientation = 'h', xanchor = 'center',x = 0.5,y = 1.20,font=dict(size=12))
                        )
        fig.show()
        fig.write_html(
                        "/lakehouse/default/Files/MLApproach_k-means_elbowChart.html",
                        include_plotlyjs="cdn",
                        full_html=True)
    elif test_result =='N':
        print('===============================================================')
        print('INFO: As test = N, Running K-Means Algorithm against dataset.')
        print('===============================================================')
        k_selection = k
        if k_selection is None:
            raise ValueError('ERROR: As test = Y, you need to provide a valid integer number for k. Run this function with test = N to find it :)')
        elif not isinstance(k_selection,int):
            raise ValueError('ERROR: Please provide a valid integer number for k')
        else:
            print('===============================================================')
            print('INFO: All parameters for k means model accepted, running now..')
            print('===============================================================')
            kmeans = KMeans(featuresCol=featurescol,k=k_selection)
            k_means_model = kmeans.fit(feature_data)
            k_means_predictions = k_means_model.transform(feature_data)
            k_means_predictions = k_means_predictions.withColumnRenamed('prediction','k_means_prediction')
            print('===============================================================')
            print('INFO: Producing PCA Plot to show clusters')
            print('===============================================================')
            pca = PCA(k=2,inputCol=featurescol, outputCol='pcaFeatures')
            pca_model = pca.fit(k_means_predictions)
            pca_result = pca_model.transform(k_means_predictions)
            pdf = pca_result.select("pcaFeatures","k_means_prediction").toPandas()
            pdf["pc1"] = pdf["pcaFeatures"].apply(lambda v: float(v[0]))
            pdf["pc2"] = pdf["pcaFeatures"].apply(lambda v: float(v[1]))
            fig_pca = px.scatter(
                                    pdf, x="pc1", y="pc2",
                                    color="k_means_prediction",
                                    title=f"KMeans Clusters (k={k_selection}) visualized with PCA (Principal Component Analysis)",
                                    labels={"pc1":"PC1","pc2":"PC2"},
                                    color_continuous_scale="Viridis"
                                )
            fig_pca.update_layout(plot_bgcolor='white')
            fig_pca.show()

            fig_pca.write_html(
                "/lakehouse/default/Files/MLApproach_k-means_PCAChart.html",
                include_plotlyjs="cdn",
                full_html=True)
            print('===============================================================')
            print('INFO: Producing t-SNE plot to show clusters')
            print('===============================================================')
            tsne = TSNE(n_components=2, random_state=27, perplexity=30)
            X_tsne = tsne.fit_transform(pdf[["pc1","pc2"]].values)
            pdf["tsne1"] = X_tsne[:,0]
            pdf["tsne2"] = X_tsne[:,1]
            fig_tsne = px.scatter(
                pdf, x="tsne1", y="tsne2",
                color="k_means_prediction",
                title=f"KMeans Clusters (k={k}) visualized with t-SNE (t-distributed stochastic neighbor embedding)",
                labels={"tsne1":"t-SNE1","tsne2":"t-SNE2"},
                color_continuous_scale="Viridis"
            )
            fig_tsne.update_layout(plot_bgcolor="white")
            fig_tsne.show()

            fig_tsne.write_html(
            "/lakehouse/default/Files/MLApproach_k-means_T-SNEChart.html",
            include_plotlyjs="cdn",
            full_html=True)

            print('===============================================================')
            print('INFO: kmeans algorithm complete and saved to variable')
            print('===============================================================')
            return k_means_predictions
    else:
        raise ValueError('ERROR: test parameter needs a Y or N')
