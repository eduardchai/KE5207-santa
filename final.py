import time
import random
import utils
import multiprocessing
import redis
import json
from haversine import haversine

NORTH_POLE_LAT = 90
NORTH_POLE_LONG = 0
NORTH_POLE = (90, 0)
SLEIGH_CAPACITY = 1000
INITIAL_POPULATION_SIZE = 200
ELITE_RATE = 0.3
ELITE_NUM = INITIAL_POPULATION_SIZE * ELITE_RATE
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


def trip_weight_util(gift_list):
    total_weight = 0

    for gift_id in gift_list:
        total_weight += float(REDIS_CLIENT.hget('weight-lookup', f'gift-{gift_id}'))

    return total_weight


def trip_cost(gift_list):
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
    weariness = 0
    count = 0
    for trip in ind:
        count += 1
        weariness += trip_cost(trip)
        if count % 100 == 0:
            print(weariness)
    return weariness


def get_gifts_checklist(gift_list):
    gifts_checklist = {}
    for i in gift_list:
        gifts_checklist[i] = 0
    return gifts_checklist


def get_available_gifts(gifts):
    return [key for key, value in gifts.items() if not value]


def generate_trips(gift_list):
    gifts_checklist = get_gifts_checklist(gift_list)
    available_gifts = get_available_gifts(gifts_checklist)

    trip_list = []
    while len(available_gifts) > 0:
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
                    # print(f'end_trip with total_weight: {trip_total_weight}')
                    end_trip = True

        trip_list.append(trip_gifts_list)
        available_gifts = get_available_gifts(gifts_checklist)

    return trip_list


def fit_gifts_to_trip(gift_list, trip):
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
                if curr_weight + gift_weight <= 1000:
                    curr_weight += gift_weight
                    curr_gift_list.append(gift_id)
                    last_delivered_gift_id = gift_id
                    gift_list.remove(gift_id)
                    found = True
                    break

        if not found:
            break

    return gift_list, { 'gift_list': curr_gift_list, 'total_weight': curr_weight}


def crossover(parent_a_ori, parent_b_ori):
    parent_a = parent_a_ori['trip_list'].copy()
    parent_b = parent_b_ori['trip_list'].copy()

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
        additional_trips = generate_trips(undelivered_gifts_list)
        offspring_list += additional_trips

    offspring_cost = total_weariness(offspring_list)

    return offspring_list, offspring_cost


def crossover_wrapper(i, population=[]):
    offsprings_count = int(len(population) / 2)
    offspring_index = i * offsprings_count

    print(f'worker:{i} pop_size:{len(population)}')
    for parent_a, parent_b in zip(population[::2], population[1::2]):
        start_cx = time.time()

        trip_list, total_cost = crossover(parent_a, parent_b)

        REDIS_CLIENT.hset(f'off-{offspring_index}', 'trip_list', json.dumps(trip_list))
        REDIS_CLIENT.hset(f'off-{offspring_index}', 'total_cost', total_cost)

        offspring_index += 1
        end_cx = time.time()
        print(f'worker:{i} offspring_index:{offspring_index} crossover time elapsed:{end_cx-start_cx}')


def tournament_selection(population, tournament_size, k):
    participant_ids = random.sample(range(0, len(population)), tournament_size)

    participants = []
    for ind_id in participant_ids:
        participants.append((ind_id, population[ind_id]['total_cost']))

    sorted_by_fitness = sorted(participants, key=lambda tup: tup[1])

    return sorted_by_fitness[:k]


def chunks(l, n):
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


if __name__ == '__main__':
    start = time.time()

    new_generation = []

    # 1. GET INITIAL POPULATION
    initial_population = []
    for i in range(INITIAL_POPULATION_SIZE):
        trip_list = json.loads(REDIS_CLIENT.hget(f'ind-{i}', 'trip_list'))
        total_cost = float(REDIS_CLIENT.hget(f'ind-{i}', 'total_cost'))
        initial_population.append({
            'trip_list': trip_list,
            'total_cost': total_cost
        })

    # 2. KEEP ELITE INDIVIDUAL
    sorted_by_fitness = sorted(initial_population, key=lambda obj: obj['total_cost'])
    new_generation += sorted_by_fitness[:int(ELITE_NUM)]

    # 3. POPULATION SELECTION BY TOURNAMENT
    SELECTION_NUM = INITIAL_POPULATION_SIZE * (1 - ELITE_RATE*2)
    NUM_OF_WINNERS_PER_TOURNAMENT = 2
    TOURNAMENT_SIZE = 30
    selected_population = []
    for _ in range(int(SELECTION_NUM)):
        winners = tournament_selection(initial_population, TOURNAMENT_SIZE, NUM_OF_WINNERS_PER_TOURNAMENT)
        for winner in winners:
            winner_id = winner[0]
            selected_population.append({
                'trip_list': initial_population[winner_id]['trip_list'],
                'total_cost': initial_population[winner_id]['total_cost']
            })

    # 3. CROSSOVER SELECTED POPULATION
    chunked_selection = chunks(selected_population, multiprocessing.cpu_count()-1)
    jobs = []
    for i in range(len(chunked_selection)):
        selection_chunk = chunked_selection[i]
        p = multiprocessing.Process(target=crossover_wrapper, args=(i,), kwargs={'population': selection_chunk})
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()

    end = time.time()
    print(f'time_elapsed:{end-start}')

    # generate_new_population(0)
    # jobs = []
    # for i in range(multiprocessing.cpu_count()-1):
    #     p = multiprocessing.Process(target=generate_new_population, args=(i,))
    #     jobs.append(p)
    #     p.start()

# pool = multiprocessing.Pool(processes = 1)
# iterations = list(range(MAX_ITERATIONS))

# pool.map(generate_new_population, iterations)
# pool.close()
# pool.join()