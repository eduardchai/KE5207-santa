import time
import random
import utils
import os
import multiprocessing
import redis
import json
import scipy as sp
from haversine import haversine

NORTH_POLE_LAT = 90
NORTH_POLE_LONG = 0
SLEIGH_CAPACITY = 1000
INITIAL_POPULATION_SIZE = 200
MAX_ITERATIONS = 100000
EMPTY_SLEIGH_WEIGHT = 10
TOP_K = int(INITIAL_POPULATION_SIZE * 0.1) if int(INITIAL_POPULATION_SIZE * 0.1 > 10) else 10

REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def sort_population_by_fitness():
    initial_population_index = []
    for i in range(INITIAL_POPULATION_SIZE):
        initial_population_index.append((i, float(REDIS_CLIENT.hget(f'ind-{i}', 'total_cost'))))
    sort_by_fitness = sorted(initial_population_index, key=lambda tup: tup[1])
    return sort_by_fitness


def tripWeightUtil(gift_list):
    total_weight = 0

    for gift_id in gift_list:
        total_weight += float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))

    return total_weight


def tripCost(gift_list):
    total_weight = tripWeightUtil(gift_list) + EMPTY_SLEIGH_WEIGHT

    curr_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_list[0]}'))
    curr_point = (float(curr_point['latitude']), float(curr_point['longitude']))
    curr_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_list[0]}'))

    weighted_distance = total_weight * haversine((NORTH_POLE_LAT, NORTH_POLE_LONG), curr_point)

    for gift_id in gift_list[1:]:
        remaining_weight = total_weight - curr_weight
        next_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_id}'))
        next_point = (float(next_point['latitude']), float(next_point['longitude']))

        distance = haversine(curr_point, next_point)
        weighted_distance = weighted_distance + (remaining_weight * distance)

        curr_point = next_point
        total_weight = remaining_weight
        curr_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))

    returning_distance = haversine(curr_point, (NORTH_POLE_LAT, NORTH_POLE_LONG))
    weighted_distance = weighted_distance + (EMPTY_SLEIGH_WEIGHT * returning_distance)

    return weighted_distance


def mutate_individual(trip_list, total_cost):
    mutation_count = int(len(trip_list) * 0.01)
    for i in range(mutation_count):
        trip_index = random.randint(0, len(trip_list) - 1)
        chosen_trip = trip_list[trip_index]
        trip_cost = tripCost(chosen_trip)

        swapped_gift_list = utils.mutateGiftList(chosen_trip)
        temp_trip_cost = tripCost(swapped_gift_list)
        if temp_trip_cost < trip_cost:
            print('mutation is possible!')
            print('old_total_cost', str(total_cost))
            total_cost = total_cost - trip_cost + temp_trip_cost
            print('new_total_cost', str(total_cost))
            trip_list[trip_index] = swapped_gift_list

    return trip_list, total_cost

def crossover(trip_list, total_cost):
    crossover_count = int(len(trip_list) * 0.01)
    for i in range(crossover_count):
        trip_index_a, trip_index_b = utils.generateSwapIndices(len(trip_list))

        gift_list_a = trip_list[trip_index_a]
        gift_list_b = trip_list[trip_index_b]

        gift_list_a_total_len = len(gift_list_a)
        gift_list_b_total_len = len(gift_list_b)

        gift_list_a_half_len = int(gift_list_a_total_len / 2)
        gift_list_b_half_len = int(gift_list_b_total_len / 2)

        gift_list_a_half_1 = gift_list_a[:gift_list_a_half_len]
        gift_list_a_half_2 = gift_list_a[gift_list_a_half_len:]

        gift_list_b_half_1 = gift_list_b[:gift_list_b_half_len]
        gift_list_b_half_2 = gift_list_b[gift_list_b_half_len:]

        success = False
        while not success:
            new_gift_list_a = gift_list_a_half_1 + gift_list_b_half_2
            new_gift_list_b = gift_list_a_half_2 + gift_list_b_half_1

            new_gift_list_a_weight = tripWeightUtil(new_gift_list_a)
            new_gift_list_b_weight = tripWeightUtil(new_gift_list_b)

            if new_gift_list_a_weight <= SLEIGH_CAPACITY and new_gift_list_b_weight <= SLEIGH_CAPACITY:
                success = True
            else:
                gift_list_a_half_len += 1
                gift_list_b_half_len -= 1
                if gift_list_a_half_len >= gift_list_a_total_len or gift_list_b_half_len >= gift_list_b_total_len:
                    success = True
                else:
                    gift_list_a_half_1 = gift_list_a[:gift_list_a_half_len]
                    gift_list_a_half_2 = gift_list_a[gift_list_a_half_len:]
                    gift_list_b_half_1 = gift_list_b[:gift_list_b_half_len]
                    gift_list_b_half_2 = gift_list_b[gift_list_b_half_len:]

        # Check if new gene is better
        gift_list_a_cost = tripCost(gift_list_a)
        gift_list_b_cost = tripCost(gift_list_b)

        new_gift_list_a_cost = tripCost(new_gift_list_a)
        new_gift_list_b_cost = tripCost(new_gift_list_b)

        crossover_final_cost = (new_gift_list_a_cost - gift_list_a_cost) + (new_gift_list_b_cost - gift_list_b_cost)
        if crossover_final_cost < 0:
            print('crossover is possible!')
            trip_list[trip_index_a] = new_gift_list_a
            trip_list[trip_index_b] = new_gift_list_b
            print('old_total_cost', str(total_cost))
            total_cost += crossover_final_cost
            print('new_total_cost', str(total_cost))
        
    return trip_list, total_cost

def generate_new_population(i):
    iteration = 0
    sp.random.seed()
    while True:
        iteration += 1
        start = time.time()
        sorted_population = sort_population_by_fitness()

        curr_best_total_cost = sorted_population[0][1]
        
        mutation_method = random.random()
        if mutation_method < 0.5:
            picked_index = random.randint(INITIAL_POPULATION_SIZE - TOP_K, INITIAL_POPULATION_SIZE - 1) # LAST 10
        else:
            picked_index = random.randint(0, TOP_K - 1) # TOP 10
        
        chosen_index = sorted_population[picked_index][0]
        chosen_ind_trip_list = json.loads(REDIS_CLIENT.hget(f'ind-{chosen_index}', 'trip_list'))
        chosen_ind_total_cost = float(REDIS_CLIENT.hget(f'ind-{chosen_index}', 'total_cost'))
        
        mutation_rate = random.random()
        if mutation_rate <= 0.3:
            chosen_ind_trip_list, chosen_ind_total_cost = mutate_individual(chosen_ind_trip_list, chosen_ind_total_cost)

        crossover_rate = random.random()
        # if crossover_rate <= 0.3:
        chosen_ind_trip_list, chosen_ind_total_cost = crossover(chosen_ind_trip_list, chosen_ind_total_cost)

        REDIS_CLIENT.hset(f'ind-{chosen_index}', 'trip_list', json.dumps(chosen_ind_trip_list))
        REDIS_CLIENT.hset(f'ind-{chosen_index}', 'total_cost', chosen_ind_total_cost)

        if chosen_ind_total_cost < curr_best_total_cost:
            end = time.time()
            print(f'worker: {i} iteration: {iteration} best_score: {sorted_population[0][1]} time elapsed: {end-start}')

if __name__ == '__main__':
    sorted_population = sort_population_by_fitness()
    print('best solution', sorted_population[0])

    jobs = []
    for i in range(multiprocessing.cpu_count()-1):
        p = multiprocessing.Process(target=generate_new_population, args=(i,))
        jobs.append(p)
        p.start()

# pool = multiprocessing.Pool(processes = 1)
# iterations = list(range(MAX_ITERATIONS))

# pool.map(generate_new_population, iterations)
# pool.close()
# pool.join()