import time
import random
import utils
import os
import multiprocessing
import redis
import json
import scipy as sp

SLEIGH_CAPACITY = 1000
INITIAL_POPULATION_SIZE = 200
MAX_ITERATIONS = 100000
TOP_K = int(INITIAL_POPULATION_SIZE * 0.1) if int(INITIAL_POPULATION_SIZE * 0.1 > 10) else 10

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def sort_population_by_fitness():
    initial_population_index = []
    for i in range(INITIAL_POPULATION_SIZE):
        initial_population_index.append((i, float(redis_client.hget(f'ind-{i}', 'total_cost'))))
    sort_by_fitness = sorted(initial_population_index, key=lambda tup: tup[1])
    return sort_by_fitness

def mutate_individual(trip_list, total_cost):
    mutation_count = int(len(trip_list) * 0.01)
    for i in range(mutation_count):
        trip_index = random.randint(0, len(trip_list) - 1)
        chosen_trip = trip_list[trip_index]
        swapped_gift_list = utils.mutateGiftList(chosen_trip['gift_list'])
        temp_trip_cost = utils.tripCost(swapped_gift_list)

        if temp_trip_cost < chosen_trip['trip_cost']:
            total_cost = total_cost - chosen_trip['trip_cost'] + temp_trip_cost
            trip_list[trip_index]['gift_list'] = swapped_gift_list
            trip_list[trip_index]['trip_cost'] = temp_trip_cost
        
    return trip_list, total_cost

def crossover(trip_list, total_cost):
    crossover_count = int(len(trip_list) * 0.01)
    for i in range(crossover_count):
        trip_index_a, trip_index_b = utils.generateSwapIndices(len(trip_list))

        chosen_trip_a = trip_list[trip_index_a]
        chosen_trip_b = trip_list[trip_index_b]

        gift_list_a = chosen_trip_a['gift_list']
        gift_list_b = chosen_trip_b['gift_list']

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

            new_gift_list_a_weight = utils.tripWeightUtil(new_gift_list_a, 0, len(new_gift_list_a)-1)
            new_gift_list_b_weight = utils.tripWeightUtil(new_gift_list_b, 0, len(new_gift_list_b)-1)

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
        gift_list_a_cost = chosen_trip_a['trip_cost']
        gift_list_b_cost = chosen_trip_b['trip_cost']

        new_gift_list_a_cost = utils.tripCost(new_gift_list_a)
        new_gift_list_b_cost = utils.tripCost(new_gift_list_b)

        crossover_final_cost = (new_gift_list_a_cost - gift_list_a_cost) + (new_gift_list_b_cost - gift_list_b_cost)
        if crossover_final_cost < 0:
            trip_list[trip_index_a] = {
                'gift_list': new_gift_list_a,
                'trip_cost': new_gift_list_a_cost
            }
            trip_list[trip_index_b] = {
                'gift_list': new_gift_list_b,
                'trip_cost': new_gift_list_b_cost
            }

            total_cost += crossover_final_cost
        
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
        chosen_ind_trip_list = json.loads(redis_client.hget(f'ind-{chosen_index}', 'trip_list'))
        chosen_ind_total_cost = float(redis_client.hget(f'ind-{chosen_index}', 'total_cost'))
        
        mutation_rate = random.random()
        if mutation_rate <= 0.3:
            chosen_ind_trip_list, chosen_ind_total_cost = mutate_individual(chosen_ind_trip_list, chosen_ind_total_cost)

        chosen_ind_trip_list, chosen_ind_total_cost = crossover(chosen_ind_trip_list, chosen_ind_total_cost)
        
        redis_client.hset(f'ind-{chosen_index}', 'trip_list', json.dumps(chosen_ind_trip_list))
        redis_client.hset(f'ind-{chosen_index}', 'total_cost', chosen_ind_total_cost)

        if chosen_ind_total_cost < curr_best_total_cost:
            end = time.time()
            print(f'worker: {i} iteration: {iteration} best_score: {sorted_population[0][1]} time elapsed: {end-start}')

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