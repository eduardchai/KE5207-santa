import time
import random
import utils
import multiprocessing
import redis
import json
from haversine import haversine

NORTH_POLE = (90, 0)
EMPTY_SLEIGH_WEIGHT = 10
SLEIGH_CAPACITY = 1000

INITIAL_POPULATION_SIZE = 200
ELITE_RATE = 0.3
ELITE_NUM = INITIAL_POPULATION_SIZE * ELITE_RATE
MAX_ITERATIONS = 100

REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)


"""
===================================================================================================
UTILITY FUNCTIONS
===================================================================================================
"""


def trip_weight_util(gift_list):
    """
    Calculate total weight from list of gifts
    :param gift_list: list of gifts
    :return: total weight
    """
    total_weight = 0

    for gift_id in gift_list:
        total_weight += float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))

    return total_weight


def trip_cost(gift_list):
    """
    Calculate the weariness/cost of a trip including trip from and back to north pole
    :param gift_list: list of gifts in single trip
    :return: cost/weariness
    """
    total_weight = trip_weight_util(gift_list) + EMPTY_SLEIGH_WEIGHT

    curr_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_list[0]}'))
    curr_point = (float(curr_point['latitude']), float(curr_point['longitude']))
    curr_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_list[0]}'))

    weighted_distance = total_weight * haversine(NORTH_POLE, curr_point)

    for gift_id in gift_list[1:]:
        remaining_weight = total_weight - curr_weight
        next_point = json.loads(REDIS_CLIENT.hget('point-lookup', f'gift-{gift_id}'))
        next_point = (float(next_point['latitude']), float(next_point['longitude']))

        distance = haversine(curr_point, next_point)
        weighted_distance = weighted_distance + (remaining_weight * distance)

        curr_point = next_point
        total_weight = remaining_weight
        curr_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))

    returning_distance = haversine(curr_point, NORTH_POLE)
    weighted_distance = weighted_distance + (EMPTY_SLEIGH_WEIGHT * returning_distance)

    return weighted_distance


def total_weariness(ind):
    """
    Calculate total weariness of a individual/solution
    :param ind: individual consists of list of trips
    :return: total weariness
    """
    weariness = 0
    count = 0
    for trip in ind:
        count += 1
        weariness += trip_cost(trip)
        if count % 100 == 0:
            print(weariness)
    return weariness


def get_gifts_checklist(gift_list):
    """
    Get a dictionary of all gifts with value:
    0: not yet delivered
    1: delivered
    :param gift_list: list of gifts
    :return: checklist dictionary
    """
    gifts_checklist = {}
    for i in gift_list:
        gifts_checklist[i] = 0
    return gifts_checklist


def get_available_gifts(gifts_checklist):
    """
    Get all gifts that not yet delivered
    :param gifts_checklist: gifts checklist
    :return: a list of not yet delivered gifts
    """
    return [key for key, value in gifts_checklist.items() if not value]


def chunks(l, n):
    """
    Chunk a list into smaller list of lists
    :param l: original list
    :param n: number of chunks
    :return: list of lists
    """
    chunk_size = int(len(l)/n)
    if chunk_size % 2 > 0:
        chunk_size += 1

    pop_chunks = []
    while True:
        chunk = []
        start_index = len(pop_chunks) * chunk_size
        end_index = (len(pop_chunks) + 1) * chunk_size
        end_index = end_index if end_index < len(l) else len(l)
        for i in range(start_index, end_index):
            chunk.append(l[i])
        pop_chunks.append(chunk)

        if end_index == len(l):
            break

    return pop_chunks


"""
===================================================================================================
MUTATION ALGORITHM
===================================================================================================
"""


def mutate(mutation_id, trip_list, total_cost):
    """
    Do mutation on individual/solution
    :param trip_list: list of trips of a single individual/solution
    :param total_cost: total cost/weariness of a single individual/solution
    :return: mutated list of trips, new total cost
    """
    mutation_count = int(len(trip_list) * 0.1)
    for i in range(mutation_count):
        trip_index_a, trip_index_b = utils.generateSwapIndices(len(trip_list))

        gift_list_a = trip_list[trip_index_a]
        gift_list_b = trip_list[trip_index_b]

        gift_list_a_total_len = len(gift_list_a)
        gift_list_b_total_len = len(gift_list_b)

        gift_list_a_half_len = int(gift_list_a_total_len / 2)
        gift_list_b_half_len = int(gift_list_b_total_len / 2)

        if len(gift_list_a) > 2 and len(gift_list_b) > 2:
            gift_list_a_half_1 = gift_list_a[:gift_list_a_half_len]
            gift_list_a_half_2 = gift_list_a[gift_list_a_half_len:]

            gift_list_b_half_1 = gift_list_b[:gift_list_b_half_len]
            gift_list_b_half_2 = gift_list_b[gift_list_b_half_len:]

            success = False
            while not success:
                new_gift_list_a = gift_list_a_half_1 + gift_list_b_half_2
                new_gift_list_b = gift_list_a_half_2 + gift_list_b_half_1

                new_gift_list_a_weight = trip_weight_util(new_gift_list_a)
                new_gift_list_b_weight = trip_weight_util(new_gift_list_b)

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

            if new_gift_list_a_weight <= SLEIGH_CAPACITY and new_gift_list_b_weight <= SLEIGH_CAPACITY:
                # Check if new gene is better
                gift_list_a_cost = trip_cost(gift_list_a)
                gift_list_b_cost = trip_cost(gift_list_b)

                new_gift_list_a_cost = trip_cost(new_gift_list_a)
                new_gift_list_b_cost = trip_cost(new_gift_list_b)

                mutation_final_cost = (new_gift_list_a_cost - gift_list_a_cost) + \
                                      (new_gift_list_b_cost - gift_list_b_cost)

                if mutation_final_cost < 0:
                    print('mutation successful!')
                    trip_list[trip_index_a] = new_gift_list_a
                    trip_list[trip_index_b] = new_gift_list_b

                    total_cost += mutation_final_cost
        else:
            print('skip mutation')
    
    REDIS_CLIENT.hset(f'mutate-{mutation_id}', 'trip_list', json.dumps(trip_list))
    REDIS_CLIENT.hset(f'mutate-{mutation_id}', 'total_cost', float(total_cost))


def mutate_wrapper(i, population=[]):
    """
    Mutation wrapper helper for multiprocessing computation
    :param i: worker id
    :param population: population that need to be processed
    """
    # print(f'mutation_worker:{i} pop_size:{len(population)}')
    mutation_id = i * len(population)
    for ind in population:
        start_cx = time.time()
        mutate(mutation_id, ind['trip_list'].copy(), ind['total_cost'])
        end_cx = time.time()
        # print(f'mutation_worker:{i} mutation_id:{mutation_id} mutation time elapsed:{end_cx-start_cx}')
        mutation_id += 1


"""
===================================================================================================
CROSSOVER ALGORITHM
===================================================================================================
"""


def tournament_selection(population, tournament_size, k):
    """
    From a population of tournament_size, select best k solution
    :param population: total population
    :param tournament_size: number of random solutions picked from population
    :param k: number of best solutions picked from tournament
    :return: population of tournament best solutions/winners
    """
    participant_ids = random.sample(range(0, len(population)), tournament_size)

    participants = []
    for ind_id in participant_ids:
        participants.append((ind_id, population[ind_id]['total_cost']))

    sorted_by_fitness = sorted(participants, key=lambda tup: tup[1])

    return sorted_by_fitness[:k]


def greedy_search_trips(gift_list):
    """
    Generate trips from list of gifts using greedy search method
    :param gift_list: list of gifts
    :return: list of trips
    """

    gifts_checklist = get_gifts_checklist(gift_list)
    available_gifts = get_available_gifts(gifts_checklist)

    trip_list = []
    while len(available_gifts) > 0:
        # pick random starting point
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
                # limit iteration to 5000 to escape infinite loop
                if count < 5000:
                    next_gift_id = point[0]
                    next_distance = point[1]
                    if next_gift_id in gift_list:
                        if not gifts_checklist[next_gift_id]:
                            next_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{next_gift_id}'))
                            if trip_total_weight + next_weight <= SLEIGH_CAPACITY:
                                gift_id = next_gift_id

                                trip_gifts_list.append(gift_id)
                                trip_weight_list.append(next_weight)
                                trip_distance_list.append(next_distance)

                                trip_total_weight += next_weight
                                gifts_checklist[gift_id] = 1
                                break
                else:
                    end_trip = True

        trip_list.append(trip_gifts_list)
        available_gifts = get_available_gifts(gifts_checklist)

    return trip_list


def fit_gifts_to_trip(gift_list, trip):
    """
    Fit gifts to existing trip
    :param gift_list: list of gifts
    :param trip: dictionary of trip which consist of:
                    gift_list: list of gifts
                    total_weight: current trip weight
    :return: list of remainder gifts, trip
    """
    curr_gift_list = trip['gift_list']
    curr_weight = trip['total_weight']

    last_delivered_gift_id = curr_gift_list[-1]
    while True:
        closest_5k_points = json.loads(REDIS_CLIENT.hget('distance-lookup', f'gift-{last_delivered_gift_id}'))
        closest_5k_gift_ids = [point[0] for point in closest_5k_points]

        found = False
        for gift_id in gift_list:
            if gift_id in closest_5k_gift_ids:
                gift_weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))
                if curr_weight + gift_weight <= SLEIGH_CAPACITY:
                    curr_weight += gift_weight
                    curr_gift_list.append(gift_id)
                    last_delivered_gift_id = gift_id
                    gift_list.remove(gift_id)
                    found = True
                    break

        if not found:
            break

    return gift_list, { 'gift_list': curr_gift_list, 'total_weight': curr_weight}


def crossover(offspring_index, parent_a, parent_b):
    """
    Do crossover and get offspring from parena_a and parent_b
    :param offspring_index: index of offspring which will be used to store offspring in redis
    :param parent_a: an individual/solution that consists of list of trips
    :param parent_b: an individual/solution that consists of list of trips
    :return: Store offspring in redis with given offspring_index
    """
    offspring = []
    delivered_gifts = []
    low_weight_trip_id = []
    count = 0

    while len(parent_a) > 0 and len(parent_b) > 0 and count < 100:
        count += 1
        random_trip_a_index = random.randint(0, len(parent_a)-1)
        random_trip_b_index = random.randint(0, len(parent_b)-1)

        trip_a = parent_a[random_trip_a_index]
        trip_b = parent_b[random_trip_b_index]

        combined_trip = trip_a + trip_b
        if len(set(combined_trip)) == len(combined_trip):
            if len(set(delivered_gifts + combined_trip)) == len(delivered_gifts + combined_trip):
                trip_a_total_weight = trip_weight_util(trip_a)
                offspring.append({
                    'gift_list': trip_a,
                    'total_weight': trip_a_total_weight
                })
                if trip_a_total_weight <= 900:
                    low_weight_trip_id.append(len(offspring)-1)

                trip_b_total_weight = trip_weight_util(trip_b)
                offspring.append({
                    'gift_list': trip_b,
                    'total_weight': trip_b_total_weight
                })
                if trip_b_total_weight <= 900:
                    low_weight_trip_id.append(len(offspring)-1)

                delivered_gifts += combined_trip
                count = 0
                del parent_a[random_trip_a_index]
                del parent_b[random_trip_b_index]

    delivered_gifts_list = set([item for sublist in offspring for item in sublist['gift_list']])
    undelivered_gifts_list = set([item for sublist in parent_a for item in sublist] +
                            [item for sublist in parent_b for item in sublist]) - delivered_gifts_list

    # try to fit undelivered gifts to existing trip
    for trip_id in low_weight_trip_id:
        trip = offspring[trip_id]
        undelivered_gifts_list, new_trip = fit_gifts_to_trip(undelivered_gifts_list, trip)
        offspring[trip_id] = new_trip

    offspring_list = [trip['gift_list'] for trip in offspring]

    if len(undelivered_gifts_list) > 0:
        additional_trips = greedy_search_trips(undelivered_gifts_list)
        offspring_list += additional_trips

    offspring_cost = total_weariness(offspring_list)

    REDIS_CLIENT.hset(f'off-{offspring_index}', 'trip_list', json.dumps(offspring_list))
    REDIS_CLIENT.hset(f'off-{offspring_index}', 'total_cost', float(offspring_cost))


def crossover_wrapper(i, population=[]):
    """
    Crossover wrapper helper for multiprocessing computation
    :param i: worker id
    :param population: population that need to be processed
    """
    offsprings_count = int(len(population) / 2)
    offspring_index = i * offsprings_count

    print(f'worker:{i} pop_size:{len(population)}')
    for parent_a, parent_b in zip(population[::2], population[1::2]):
        start_cx = time.time()
        crossover(offspring_index, parent_a["trip_list"].copy(), parent_b["trip_list"].copy())
        offspring_index += 1
        end_cx = time.time()
        print(f'worker:{i} offspring_index:{offspring_index} crossover time elapsed:{end_cx-start_cx}')


if __name__ == '__main__':

    # 1. GET INITIAL POPULATION
    initial_population = []
    for i in range(INITIAL_POPULATION_SIZE):
        trip_list = json.loads(REDIS_CLIENT.hget(f'ind-{i}', 'trip_list'))
        total_cost = float(REDIS_CLIENT.hget(f'ind-{i}', 'total_cost'))
        initial_population.append({
            'trip_list': trip_list,
            'total_cost': total_cost
        })

    for step in range(MAX_ITERATIONS):
        start = time.time()
        new_generation = []
        # 2. KEEP ELITE INDIVIDUAL
        sorted_by_fitness = sorted(initial_population, key=lambda obj: obj['total_cost'])
        new_generation += sorted_by_fitness[:int(ELITE_NUM)]

        # 3. POPULATION SELECTION BY TOURNAMENT
        # Here we multiply ELITE RATE by 2 because the other half will be used for mutation
        SELECTION_NUM = INITIAL_POPULATION_SIZE * (1 - ELITE_RATE*2)
        NUM_OF_WINNERS_PER_TOURNAMENT = 2
        TOURNAMENT_SIZE = 30
        cx_population = []
        for _ in range(int(SELECTION_NUM)):
            winners = tournament_selection(initial_population, TOURNAMENT_SIZE, NUM_OF_WINNERS_PER_TOURNAMENT)
            for winner in winners:
                winner_id = winner[0]
                cx_population.append({
                    'trip_list': initial_population[winner_id]['trip_list'],
                    'total_cost': initial_population[winner_id]['total_cost']
                })

        # 4. CROSSOVER SELECTED POPULATION
        # chunked_selection = chunks(cx_population, multiprocessing.cpu_count()-1)
        # jobs = []
        # for i in range(len(chunked_selection)):
        #     selection_chunk = chunked_selection[i]
        #     p = multiprocessing.Process(target=crossover_wrapper, args=(i,), kwargs={'population': selection_chunk})
        #     jobs.append(p)
        #     p.start()
        #
        # for job in jobs:
        #     job.join()


        # 5. CONSTRUCT POPULATION FOR MUTATION
        # Population consists of population from crossover and elites population
        mutation_population = new_generation.copy()
        for i in range(int(len(cx_population)/2)):
            trip_list = json.loads(REDIS_CLIENT.hget(f'off-{i}', 'trip_list'))
            total_cost = float(REDIS_CLIENT.hget(f'off-{i}', 'total_cost'))
            mutation_population.append({
                'trip_list': trip_list,
                'total_cost': total_cost
            })

        # 6. MUTATION
        mutation_chunks = chunks(mutation_population, multiprocessing.cpu_count()-1)
        mutation_jobs = []
        for i in range(len(mutation_chunks)):
            selection_chunk = mutation_chunks[i]
            p = multiprocessing.Process(target=mutate_wrapper, args=(i,), kwargs={'population': selection_chunk})
            mutation_jobs.append(p)
            p.start()

        for job in mutation_jobs:
            job.join()

        # 7. COMBINED MUTATED POPULATION WITH ELITES
        for i in range(len(mutation_population)):
            trip_list = json.loads(REDIS_CLIENT.hget(f'mutate-{i}', 'trip_list'))
            total_cost = float(REDIS_CLIENT.hget(f'mutate-{i}', 'total_cost'))
            new_generation.append({
                'trip_list': trip_list,
                'total_cost': total_cost
            })

        # 8. STORE NEW GENERATION TO REDIS
        new_id = 0
        for ind in new_generation:
            trip_list = ind['trip_list']
            total_cost = ind['total_cost']
            REDIS_CLIENT.hset(f'new-{new_id}', 'trip_list', json.dumps(trip_list))
            REDIS_CLIENT.hset(f'new-{new_id}', 'total_cost', float(total_cost))
            new_id += 1

        sorted_by_fitness = sorted(new_generation, key=lambda obj: obj['total_cost'])
        print(f'step:{step} best solution: {sorted_by_fitness[0]["total_cost"]}')

        end = time.time()
        print(f'step:{step} time_elapsed:{end-start}')

        initial_population = new_generation
