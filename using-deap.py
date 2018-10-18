import random
import redis
import json

from deap import base
from deap import creator
from deap import tools
from haversine import haversine


REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)
EMPTY_SLEIGH_WEIGHT = 10
NORTH_POLE = (90, 0)

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
    for trip in ind:
        weariness += trip_cost(trip)
        print(weariness)
    return weariness


def generate_population(ind_class):
    picked_index = random.randint(0, 299)
    chosen_ind_trip_list = json.loads(REDIS_CLIENT.hget(f'ind-{picked_index}', 'trip_list'))
    chosen_ind_total_cost = float(REDIS_CLIENT.hget(f'ind-{picked_index}', 'total_cost'))

    ind = ind_class(chosen_ind_trip_list)
    ind.fitness.values = (chosen_ind_total_cost,)

    return ind

def get_outer_population(n):
    outer_pop = []
    for i in range(n):
        ind_trip_list = json.loads(REDIS_CLIENT.hget(f'ind-{i}', 'trip_list'))
        ind_total_cost = float(REDIS_CLIENT.hget(f'ind-{i}', 'total_cost'))
        outer_pop.append({
            'trip_list': ind_trip_list,
            'trip_cost': ind_total_cost
        })

    return outer_pop

def generate_inner_population(outer_ind):
    chosen_ind_trip_list = outer_ind['trip_list']
    return chosen_ind_trip_list


def create_individual(ind_class, gift_list):
    return ind_class(gift_list)


creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)

outer_pop = get_outer_population(5)
toolbox = base.Toolbox()
for outer_ind in outer_pop:
    inner_pop = generate_inner_population(outer_ind)
    for ind in inner_pop:
        toolbox.register("individual", create_individual, creator.Individual, gift_list=ind)
        toolbox.register('population', tools.initIterate, list, toolbox.individual)
        pop = toolbox.population(n=10)
        print(ind)


IND_SIZE=10

toolbox = base.Toolbox()
# toolbox.register("individual", generate_population, creator.Individual)
toolbox.register("individual", generate_inner_population, creator.Individual, outer_pop[0]['trip_list'])
toolbox.register('population', tools.initRepeat, list, toolbox.individual)
#
# toolbox.register("evaluate", total_weariness)
# toolbox.register("mate", tools.cxPartialyMatched)
# toolbox.register("mutate", tools.mutShuffleIndexes, indpb = 0.05)
# toolbox.register("select", tools.selTournament, tournsize = 3)
#
# pop = toolbox.population(n=10)
# fitnesses = list(map(toolbox.evaluate, pop))
# for ind, fit in zip(pop, fitnesses):
#     print(ind)
#     ind.fitness.values = fit

ind = toolbox.individual()
print(ind)
# NGEN = 5
# CXPB = 0.7
# for g in range(NGEN):
#     print("-- Generation %i --" % g)
#     # Select the next generation individuals
#     offspring = toolbox.select(pop, len(pop))
#     # Clone the selected individuals
#     offspring = list(map(toolbox.clone, offspring))
#
#     # Apply crossover and mutation on the offspring
#     for child1, child2 in zip(offspring[::2], offspring[1::2]):
#         if random.random() < CXPB:
#             toolbox.mate(child1, child2)
