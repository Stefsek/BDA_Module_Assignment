import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from kneed import KneeLocator

def avg_silhouette_scores(scaled_data, k_range, n_init, max_iter, random_state):
    """
    Calculates average silhouette scores for a range of cluster numbers.

    Args:
        scaled_data (array-like): The pre-processed data for clustering.
        k_range (range): A range of cluster numbers to evaluate.
        n_init (int): Number of time the k-means algorithm will be run with different centroid seeds.
        max_iter (int): Maximum number of iterations for each run of k-means.
        random_state (int): Determines random number generation for centroid initialization.

    Returns:
        list: A list of average silhouette scores for each cluster number in k_range.
    """
    silhouette_avg_scores = []
    k_values = k_range

    # Evaluate silhouette score for each k
    for k in k_values:
        kmeans = KMeans(n_clusters=k, n_init=n_init, max_iter=max_iter, random_state=random_state)
        labels = kmeans.fit_predict(scaled_data)
        silhouette_avg = silhouette_score(scaled_data, labels)
        silhouette_avg_scores.append(silhouette_avg)

    return silhouette_avg_scores

def plot_avg_silhouette_scores(k_range, scores):
    """
    Plots the average silhouette scores over a range of k values.

    Args:
        k_range (range): A range of k values.
        scores (list): List of average silhouette scores corresponding to each k value.
    """
    k_values = k_range
    plt.figure(figsize=(8, 6))
    plt.plot(k_values, scores, marker='o')
    plt.title('Average Silhouette Scores for Different Number of Clusters (k)')
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Average silhouette score')
    plt.grid(True)
    plt.show()

def sse_calculation(scaled_data, k_range, n_init, max_iter, random_state):
    """
    Calculates the sum of squared distances (SSE) for a range of cluster numbers.

    Args:
        scaled_data (array-like): The pre-processed data for clustering.
        k_range (range): A range of cluster numbers to evaluate.
        n_init (int): Number of time the k-means algorithm will be run with different centroid seeds.
        max_iter (int): Maximum number of iterations for each run of k-means.
        random_state (int): Determines random number generation for centroid initialization.

    Returns:
        list: A list of SSE values for each cluster number in k_range.
    """
    sse = []
    k_values = k_range

    # Calculate SSE for each k
    for k in k_values:
        kmeans = KMeans(n_clusters=k, n_init=n_init, max_iter=max_iter, random_state=random_state)
        kmeans.fit(scaled_data)
        sse.append(kmeans.inertia_)

    return sse

def plot_kneelocator(sse_list, k_range, curve, direction):
    """
    Plots the sum of squared distances (SSE) and indicates the optimal number of clusters using KneeLocator.

    Args:
        sse_list (list): List of SSE values for each cluster number.
        k_range (range): A range of cluster numbers.
        curve (str): The shape of the curve ('concave' or 'convex').
        direction (str): The direction of the knee ('increasing' or 'decreasing').
    """
    k_values = k_range
    kl = KneeLocator(k_values, sse_list, curve=curve, direction=direction)
    plt.figure(figsize=(8, 6))
    plt.plot(k_values, sse_list, '-o')
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Sum of squared distances (SSE)')
    plt.title('Knee Locator for Optimal k')
    plt.axvline(x=kl.elbow, color='r', linestyle='--')
    plt.show()