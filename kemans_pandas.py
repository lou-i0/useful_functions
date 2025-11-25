import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE

def kmeans_clustering_pandas(df: pd.DataFrame, features: list, test: str = 'Y', k: int = None):
    """
    Perform K-Means clustering on a Pandas DataFrame.
    
    ARGS:
        df       : Pandas DataFrame with features already numeric (scaled/encoded if needed).
        features : List of column names to use as features.
        test     : 'Y' to run silhouette scores for k=2..9 and plot elbow chart.
                   'N' to run clustering with provided k and visualize with PCA and t-SNE.
        k        : Number of clusters (required if test='N').
    
    RETURNS:
        If test='Y': plots silhouette scores for different k.
        If test='N': returns DataFrame with cluster predictions and plots PCA/t-SNE.
    """
    X = df[features].values
    
    if test == 'Y':
        print("===============================================================")
        print("INFO: As test = Y, Performing test to find optimal k for model and dataset.")
        print("===============================================================\n")

        silhouette_scores = []
        for i in range(2, 10):
            kmeans = KMeans(n_clusters=i, random_state=42)
            preds = kmeans.fit_predict(X)
            score = silhouette_score(X, preds)
            silhouette_scores.append(score)
            print(f"Silhouette Score for k={i}: {score:.4f}")

        # Plot elbow/silhouette chart
        fig = px.line(y=silhouette_scores, x=range(2, 10),
                      title="Finding best k cluster based on Silhouette Score/Elbow method")
        fig.update_layout(plot_bgcolor='white',
                          xaxis=dict(title='K'),
                          yaxis=dict(title='Silhouette Score'))
        fig.show()

    elif test == 'N':
        print("===============================================================")
        print("INFO: As test = N, Running K-Means Algorithm against dataset.")
        print("===============================================================")
        
        if k is None or not isinstance(k, int):
            raise ValueError("ERROR: Please provide a valid integer number for k when test='N'")
        
        kmeans = KMeans(n_clusters=k, random_state=42)
        df['k_means_prediction'] = kmeans.fit_predict(X)

        # PCA visualization
        print("INFO: Producing PCA Plot to show clusters")
        pca = PCA(n_components=2)
        pcs = pca.fit_transform(X)
        df['pc1'], df['pc2'] = pcs[:,0], pcs[:,1]

        fig_pca = px.scatter(df, x="pc1", y="pc2", color="k_means_prediction",
                             title=f"KMeans Clusters (k={k}) visualized with PCA",
                             labels={"pc1":"PC1","pc2":"PC2"},
                             color_continuous_scale="Viridis")
        fig_pca.update_layout(plot_bgcolor='white')
        fig_pca.show()

        # t-SNE visualization
        print("INFO: Producing t-SNE Plot to show clusters")
        tsne = TSNE(n_components=2, random_state=27, perplexity=30)
        tsne_result = tsne.fit_transform(X)
        df['tsne1'], df['tsne2'] = tsne_result[:,0], tsne_result[:,1]

        fig_tsne = px.scatter(df, x="tsne1", y="tsne2", color="k_means_prediction",
                              title=f"KMeans Clusters (k={k}) visualized with t-SNE",
                              labels={"tsne1":"t-SNE1","tsne2":"t-SNE2"},
                              color_continuous_scale="Viridis")
        fig_tsne.update_layout(plot_bgcolor='white')
        fig_tsne.show()

        print("INFO: kmeans algorithm complete and saved to DataFrame")
        return df

    else:
        raise ValueError("ERROR: test parameter needs to be 'Y' or 'N'")
