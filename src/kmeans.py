# -*- coding: utf-8 -*-

import numpy as np
import collections
import bisect

Center = collections.namedtuple(
    "Center",
    ("start_lon", "start_lat", "stop_lon", "stop_lat"),
)

def get_random_centers(generator, k):
    """Take k values randomly from generator

    Parameters
    ----------
    generator : iterable
                Iterable where values are taken
    k : int
        Number of values to take

    Returns
    -------
    list
        list of taked values.
    """

    centers = []
    u = []
    for trip in generator:
        c = Center(trip.start_lon, trip.start_lat, trip.stop_lon, trip.stop_lat)
        ux = np.random.uniform()
        if len(u) < k:
            w = bisect.bisect(u, ux)
            u.insert(w, ux)
            centers.insert(w, c)
            continue
        if ux < u[-1]:
            u = u[:-1]
            centers = centers[:-1]
            w = bisect.bisect(u, ux)
            u.insert(w, ux)
            centers.insert(w, c)
    return centers

RT = 6371e3
dist = lambda lon1, lat1, lon2, lat2: (
        2
        * np.pi
        * RT
        * np.sqrt(
            ((lat1 - lat2) / 360) ** 2
            + ((lon1 - lon2) / 360 * np.cos((lat1 + lat2) / 360 * np.pi)) ** 2
        )
)

def classification(n_clusters, centers, generator):
    """Classify data with the closest center.

    Parameters
    ----------
    n_clusters : int
                 Number of center

    generator : iterable
                Iterable where values are taken
    """
    for trip in generator:
        min = np.inf
        label = n_clusters
        for c, k in zip(centers, range(n_clusters)):
            distance = np.sqrt(dist(c.start_lon, c.start_lat, trip.start_lon, trip.start_lat)**2
                + dist(c.stop_lon, c.stop_lat, trip.stop_lon, trip.stop_lat)**2)
            if distance < min:
                min = distance
                label = k
        yield [trip, label, min]

class KMeans():
    """Implement Kmeans algorithm."""
    def __init__(self, n_clusters=8, n_init=10, tol=1e-4, max_iter=300):
        self.n_clusters = n_clusters
        self.n_init = n_init
        self.tol = tol
        self.max_iter = max_iter

    def fit(self, func_generator):
        """Determine centers to summarize informations.

        Parameters
        ----------
        func_generator : function
                         Gives an iterable where values are taken.
        """
        n_init = self.n_init
        n_clusters = self.n_clusters
        max_iter = self.max_iter
        tol = self.tol

        centers_opt = None
        partition_opt = None
        d_opt = float("inf")

        for i in range(n_init):
            centers = get_random_centers(func_generator(), n_clusters)
            ponderations = np.zeros(n_clusters)

            step = tol + 1
            it = 0

            while step > tol and it < max_iter:
                old_centers = centers

                for k in range(n_clusters):
                    sum = np.zeros(4)
                    for trip, label, distance in classification(n_clusters, centers, func_generator()):
                        if label == k:
                            sum += np.array([trip.start_lon, trip.start_lat, trip.stop_lon, trip.stop_lat])
                            ponderations[k] += 1

                    centers[k] = Center(sum[0]/ponderations[k], sum[1]/ponderations[k], sum[2]/ponderations[k], sum[3]/ponderations[k])

                step = ((to_number_array(old_centers) - to_number_array(centers)) ** 2).sum()
                it += 1

            d_tot = 0
            for trip, label, distance in classification(n_clusters, centers, func_generator()):
                d_tot += distance**2
 
            # d_tot = np.sqrt(d_tot) voir si nécessaire 

            if d_tot < d_opt:
                centers_opt = centers
                d_opt = d_tot
                ponderations_opt = ponderations

        self.cluster_centers_ = centers_opt
        self.ponderations_ = ponderations_opt

def to_number_array(centers):
    """Turn an array of Centers into an array of numbers.

    Parameters
    ----------
    centers : array-like of shape (n_clusters, 1)
              Centers to transform.

    Return
    ------
    to_return : array-like of shape (n_clusters, 4)    
    """
    n_centers = len(centers)
    to_return = np.zeros((n_centers, 4))
    for center, k in zip(centers, range(n_centers)):
        to_return[k] = np.array([center.start_lon, center.start_lat, center.stop_lon, center.stop_lat])
    return to_return
