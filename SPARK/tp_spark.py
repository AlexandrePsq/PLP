# Alexandre PASQUIOU
# Cours de PLP - Spark - Python


import json
import math
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()

#----------------------------------------------------------------------------

###------------------------------- EXERCICE 1 -------------------------------
## Every 5s: print the empty Velib stations

# ssc = StreamingContext(sc, 5)
# stream = ssc.socketTextStream("velib.behmo.com", 9999)

# station1 = stream.map(lambda station: json.loads(station))\
# 	.filter(lambda station: station['available_bikes'] == 0)\
#     .map(lambda station: (station['contract_name'] + ' ' + station['name']))\
#     .pprint()



###------------------------------- EXERCICE 2 -------------------------------
## Every 5s: print the Velib stations that have become empty


# ssc = StreamingContext(sc, 5)
# stream = ssc.socketTextStream("velib.behmo.com", 9999)

# def become_empty(state, current_state):
# 	"""
# 	Convention:
# 	On supose qu'un 'current_state' initialement empty l'est devenu au temps 0.
# 	Remarque:
# 	On prend la moyenne de 'state', car c'est un vecteur où apparait n fois la même valeur.
# 	"""
# 	if current_state is None:
# 		current_state = (np.mean(state) == 0)
# 	if current_state == False and np.mean(state) == 0:
# 		current_state = True
# 	if current_state and np.mean(state) > 0:
# 		current_state = False
# 	return current_state

# station1 = stream.map(lambda station: json.loads(station))\
#     .map(lambda station: (station['contract_name'] + ' ' + station['name'], station['available_bikes']))\
#     .updateStateByKey(become_empty)\
#     .filter(lambda c : c[1])\
#     .transform(lambda rdd: rdd.keys())\
#     .pprint()



###------------------------------- EXERCICE 3 -------------------------------
## Every 1 min: print the stations that were most active 
## during the last 5 min (activity = number of bikes borrowed and returned)

# En supposant que les vélos ne soient (en moyenne) pas rendus et empruntés dans la même 
# fenêtre de 1 minute (car sinon l'activité d'une station n'est pas visible), on peut observer 
# l'activité d'une station en regardant la variation du nombre de 'available_bikes' dans le temps.


ssc = StreamingContext(sc, 60)
stream = ssc.socketTextStream("velib.behmo.com", 9999)

def activity(state, current_state):
	"""
	On considère 'current_state' comme étant un couple ayant pour premier élément, le dernier
	nommbre de 'available_bikes' en date, et en deuxième élément le cumul des variations 
	du nombre de 'available_bikes' dans le temps.
	"""
	delta = np.sum([abs(state[i][0] - state[i-1][0]) for i in range(1,len(state))])
	current_state = delta
	return current_state

station1 = stream.map(lambda station: json.loads(station))\
	.window(60*5,60)\
    .map(lambda station: (station['contract_name'] + ' ' + station['name'], (station['available_bikes'], station['last_update'], 0)))\
    .transform(lambda rdd: rdd.sortBy(lambda wc: wc[1][1]))\
    .updateStateByKey(activity)\
    .transform(lambda rdd: rdd.sortBy(lambda wc: -wc[1]))\
	.pprint()



#----------------------------------------------------------------------------

ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()



###--------------------------------- Mémo: -----------------------------------

## Command lines:
# nc -lk 9999
# PYSPARK_PYTHON=python3 ./bin/spark-submit ./tp_spark.py

## Data:
# ['number', 'name', 'address', 'position', 
# 'banking', 'bonus', 'status', 'contract_name', 
# 'bike_stands', 'available_bike_stands', 'available_bikes', 
# 'last_update']
