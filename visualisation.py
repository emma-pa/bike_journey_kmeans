# -*- coding: utf-8 -*-

import numpy as np
from traitement import get_trip_one_day_hour
from kmeans import KMeans
import folium
import matplotlib.pyplot as plt
import datetime

km = KMeans(n_clusters=7, n_init=1)
km.fit(get_trip_one_day_hour(datetime.datetime(2020,3,9,10)))
centers = km.cluster_centers_
ponderations = km.ponderations_
zoom = centers[0]

map_osm = folium.Map(location=[zoom.start_lat, zoom.start_lon])

for c in centers:
    map_osm.add_child(folium.RegularPolygonMarker(location=[c.start_lat,c.start_lon], fill_color='#132b5e', radius=5))
    p_start = (c.start_lat, c.start_lon)
    p_stop = (c.stop_lat, c.stop_lon)
    folium.PolyLine(locations=[p_start, p_stop], color='red').add_to(map_osm)

map_osm.save('figure_1.html')
