import time
import pandas as pd
import random
import utils
import os
import multiprocessing
import redis
import json

DATA_PATH = "./data/gifts.csv"
SLEIGH_CAPACITY = 1000
INITIAL_POPULATION_SIZE = 1000
TOP_K = int(INITIAL_POPULATION_SIZE * 0.01)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def generate_population(i):
    if (i+1) % 100 == 0:
        print("generating population", i+1)
        
    random.shuffle(gift_list)
    total_weight = 0
    total_cost = 0
    trip_list = []

    j = 0
    trip_id = 1
    while j < len(gift_list):
        gift_trip_list = []
        total_weight = 0
        while j < len(gift_list) and (total_weight + gift_list[i]['weight']) <= SLEIGH_CAPACITY:
            curr_gift = gift_list[j]
            gift_trip_list.append(curr_gift)
            total_weight = total_weight + curr_gift['weight']
            j += 1

        trip_id += 1
        trip_cost = utils.tripCost(gift_trip_list)
        trip_order = {
            'gift_list': gift_trip_list,
            'trip_cost': trip_cost
        }
        total_cost += trip_cost
        trip_list.append(trip_order)
        
    redis_client.hset(f'ind-{i}', 'trip_list', json.dumps(trip_list))
    redis_client.hset(f'ind-{i}', 'total_cost', total_cost)

# =============================
# READ DATA
# =============================
start = time.time()
print("Initiating Dataset Loading...")

df = pd.read_csv(DATA_PATH)
gift_list = []

for i in range(len(df)):
    gift_series = df.iloc[i]
    gift = {
        'gift_id': gift_series.GiftId,
        'latitude': gift_series.Latitude,
        'longitude': gift_series.Longitude,
        'weight': gift_series.Weight
    }
    gift_list.append(gift)
    
end = time.time()
print("Time Taken to load dataset:", end-start)


# =============================
# GENERATE POPULATION
# =============================
start = time.time()
print("Initiating population...")

pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
mp_initial_population = multiprocessing.Manager().list()
size_list = list(range(200, INITIAL_POPULATION_SIZE))
pool.map(generate_population, size_list)
pool.close()
pool.join()

end = time.time()
print("Time taken to init population:", end-start)