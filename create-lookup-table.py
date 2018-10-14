import time
import pandas as pd
import random
import utils
import os
import multiprocessing
import redis
import json
import gc

from haversine import haversine

# GIFT_LIST = utils.read_data()
REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def compute_gift_matrix(count):
    start = time.time()
    print(f"{count} start...")

    indices = (count+1) * 100

    for i in range(indices-100, indices):
        gift_a = GIFT_LIST[i]
        gift_id = int(gift_a['gift_id'])
        point_a = (gift_a['latitude'], gift_a['longitude'])
        gift_matrix = []
        for gift_b in GIFT_LIST:
            point_b = (gift_b['latitude'], gift_b['longitude'])
            distance = haversine(point_a, point_b)
            gift_matrix.append((int(gift_b['gift_id']), distance))

        sorted_by_distance = sorted(gift_matrix, key=lambda tup: tup[1])
        REDIS_CLIENT.hset('distance-lookup', f'gift-{gift_id}', json.dumps(sorted_by_distance[1:5001]))

        del sorted_by_distance
        gc.collect()

    end = time.time()
    print(f"{count} done in:", end-start)

def compute_gift_point(count):
    start = time.time()
    print(f"{count} start...")

    indices = (count+1) * 100

    for i in range(indices-100, indices):
        gift = GIFT_LIST[i]
        gift_id = int(gift['gift_id'])
        REDIS_CLIENT.hset('point-lookup', f'gift-{gift_id}', json.dumps({
            'latitude': gift['latitude'],
            'longitude': gift['longitude']
        }))

    end = time.time()
    print(f"{count} done in:", end-start)

def compute_gift_weight(count):
    start = time.time()
    print(f"{count} start...")

    indices = (count+1) * 100

    for i in range(indices-100, indices):
        gift = GIFT_LIST[i]
        gift_id = int(gift['gift_id'])
        REDIS_CLIENT.hset('weight-lookup', f'gift-{gift_id}', float(gift['weight']))

    end = time.time()
    print(f"{count} done in:", end-start)

def distance_lookup():
    start = time.time()
    print("Creating distance lookup table...")

    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    workers_list = list(range(0, 1000)) # number of gifts
    pool.map(compute_gift_matrix, workers_list)
    pool.close()
    pool.join()

    end = time.time()
    print("Time Taken to create distance lookup:", end-start)

def point_lookup():
    start = time.time()
    print("Creating point lookup table...")

    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    workers_list = list(range(0, 1000)) # number of gifts
    pool.map(compute_gift_point, workers_list)
    pool.close()
    pool.join()

    end = time.time()
    print("Time Taken to create point lookup:", end-start)

def weight_lookup():
    start = time.time()
    print("Creating weight lookup table...")

    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    workers_list = list(range(0, 1000)) # number of gifts
    pool.map(compute_gift_weight, workers_list)
    pool.close()
    pool.join()

    end = time.time()
    print("Time Taken to create weight lookup:", end-start)

def get_matrix(gift_id):
    matrix = REDIS_CLIENT.hget('distance-lookup', f'gift-{gift_id}')
    return json.loads(matrix)

def get_weight(gift_id):
    weight = REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}')
    return float(weight)

def get_point(gift_id):
    point = REDIS_CLIENT.hget('point-lookup', f'gift-{gift_id}')
    return json.loads(point)

if __name__ == "__main__":
    # distance_lookup()
    # weight_lookup()
    # point_lookup()
    print(get_point(1)['latitude'])
    pass