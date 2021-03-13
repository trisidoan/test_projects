#!/usr/bin/env python
# coding: utf-8

import pandas as pd, numpy as np, math
from sklearn.cluster import KMeans

from Functions import plot_functions as p, tools as t

def KMeans_within_cluster_sum_of_square(df, feature):
    ## total intra-cluster variation [or total within-cluster sum of square (WSS)]
    sse={}
    value = df[[feature]]
    for k in range(1, 10):
        kmeans = KMeans(n_clusters=k, max_iter=1000).fit(value)
        df["clusters"] = kmeans.labels_
        sse[k] = kmeans.inertia_ 

    p.quick_plot_XY(list(sse.keys()), list(sse.values()))
    return

def cluster(df, feature, k, ascending = True):
    cluster_name = feature+'Cluster'
    kmeans = KMeans(n_clusters=k)
    kmeans.fit(df[[feature]])
    df[cluster_name] = kmeans.predict(df[[feature]])
    df = t.order_cluster(cluster_name, feature,df, ascending)
    #df_cls.groupby(cluster_name)[feature].describe()
    df.boxplot(column=[feature], by=[cluster_name])
    return df