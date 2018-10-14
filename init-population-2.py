import time
import pandas as pd
import random
import utils
import os
import multiprocessing
import redis
import json
from haversine import haversine

NORTH_POLE_LAT = 90
NORTH_POLE_LONG = 0
EMPTY_SLEIGH_WEIGHT = float(10.0)
GIFTS_COUNT = 100000
SLEIGH_CAPACITY = 1000
INITIAL_POPULATION_SIZE = 300
MAX_ITERATIONS = 100000
TOP_K = int(INITIAL_POPULATION_SIZE * 0.1) if int(INITIAL_POPULATION_SIZE * 0.1 > 10) else 10

REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_gifts_checklist():
    gifts_checklist = {}
    for i in range(GIFTS_COUNT):
        gifts_checklist[i+1] = 0
    return gifts_checklist

def get_available_gifts(gifts):
    return [key for key, value in gifts.items() if not value]

def trip_cost(gift_list, total_weight, weight_list, distance_list):
    
    gift_size = len(gift_list)
    total_weight = total_weight + EMPTY_SLEIGH_WEIGHT
    starting_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_list[0]}'))
    last_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_list[-1]}'))

    returning_distance = haversine((last_point['latitude'], last_point['longitude']), (NORTH_POLE_LAT, NORTH_POLE_LONG))
    distance_list.append(returning_distance)

    weighted_distance = total_weight * haversine((NORTH_POLE_LAT, NORTH_POLE_LONG), (starting_point['latitude'], starting_point['longitude']))

    for i in range(gift_size):
        remaining_weight = total_weight - weight_list[i]
        weighted_distance = weighted_distance + (remaining_weight * distance_list[i])
        total_weight = remaining_weight
    
    return weighted_distance

def generate_population(ind_id):
    start = time.time()
    gifts_checklist = get_gifts_checklist()
    available_gifts = get_available_gifts(gifts_checklist)
    total_cost = 0

    trip_num = 1
    trip_list = []
    highest_count = 0
    while len(available_gifts) > 0:
        # print(f'trip_num: {trip_num} total_cost: {total_cost} available_gifts: {len(available_gifts)}')
        gift_index = random.randint(0, len(available_gifts) - 1)

        gift_id = available_gifts[gift_index]
        gifts_checklist[gift_id] = 1

        trip_total_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))
        trip_gifts_list = [gift_id]
        trip_weight_list = [trip_total_weight]
        trip_distance_list = []
        end_trip = False
        while trip_total_weight < SLEIGH_CAPACITY and not end_trip:
            closest_5k_points = json.loads(REDIS_CLIENT.hget('distance-lookup', f'gift-{gift_id}'))

            count = 0
            for point in closest_5k_points:
                count += 1
                if count < 5000:
                    next_gift_id = point[0]
                    next_distance = point[1]
                    if not gifts_checklist[next_gift_id]:
                        next_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{next_gift_id}'))
                        if trip_total_weight + next_weight <= SLEIGH_CAPACITY:
                            gift_id = next_gift_id
                            
                            trip_gifts_list.append(gift_id)
                            trip_weight_list.append(next_weight)
                            trip_distance_list.append(next_distance)

                            trip_total_weight += next_weight
                            gifts_checklist[gift_id] = 1

                            # to observe avg number of closest points required
                            if highest_count < count:
                                highest_count = count
                            break
                else:
                    # print(f'end_trip with total_weight: {trip_total_weight}')
                    end_trip = True
        
        trip_list.append(trip_gifts_list)
        available_gifts = get_available_gifts(gifts_checklist)
        trip_num += 1
        total_cost += trip_cost(trip_gifts_list, trip_total_weight, trip_weight_list, trip_distance_list)

    end = time.time()
    print(f'ind_id:{ind_id} highest_count:{highest_count} total_trip_num:{trip_num} total_cost:{total_cost} time_elapsed:{end-start}')
    REDIS_CLIENT.hset(f'ind-{ind_id}', 'trip_list', json.dumps(trip_list))
    REDIS_CLIENT.hset(f'ind-{ind_id}', 'total_cost', total_cost)

    return trip_list

def get_individual(ind_id):
    return json.loads(REDIS_CLIENT.hget(f'ind-{ind_id}', 'trip_list'))

# =============================
# GENERATE POPULATION
# =============================
# start = time.time()
# print("Initiating population...")

# pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
# mp_initial_population = multiprocessing.Manager().list()
# size_list = list(range(200, INITIAL_POPULATION_SIZE))
# pool.map(generate_population, size_list)
# pool.close()
# pool.join()

# end = time.time()
# print("Time taken to init population:", end-start)

if __name__ == '__main__':

    start = time.time()
    print("Initiating population...")
    
    # Try generate 1
    # generate_population(2)

    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    size_list = list(range(INITIAL_POPULATION_SIZE))
    pool.map(generate_population, size_list)
    pool.close()
    pool.join()

    end = time.time()
    print("Time taken to init population:", end-start)

    # print(len(get_individual(1)))

    