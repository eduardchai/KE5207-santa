import random
import pandas as pd
import time
from haversine import haversine

NORTH_POLE_LAT = 90
NORTH_POLE_LONG = 0
EMPTY_SLEIGH_WEIGHT = 10
SLEIGH_CAPACITY = 1000
DATA_PATH = "./data/gifts.csv"

def read_data():
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

    return gift_list

# to calculate weighted sum of distances
def tripFitness(trip_list):     
    total_cost = 0     
    for trip in trip_list:
        total_cost = total_cost + trip['trip_cost']
        
    return total_cost 

# to calculate the cumulative weight of gifts
def tripWeightUtil(gift_list, start_index, end_index):
    
    total_weight = 0 
    
    while start_index <= end_index:
        total_weight = total_weight + gift_list[start_index]['weight']
        start_index = start_index + 1
    
    return total_weight 

# to calculate the cost of the trip
def tripCost(gift_list):
    
    gift_size = len(gift_list)
    initial_gift_weight = tripWeightUtil(gift_list,0,gift_size-1) + EMPTY_SLEIGH_WEIGHT
    weighted_distance = initial_gift_weight * haversine((NORTH_POLE_LAT, NORTH_POLE_LONG), (gift_list[0]['latitude'], gift_list[0]['longitude']))
    
    for i in range(gift_size-1):
        remaining_weight = tripWeightUtil(gift_list, i+1, gift_size-1) + EMPTY_SLEIGH_WEIGHT
        distance = haversine((gift_list[i]['latitude'], gift_list[i]['longitude']), (gift_list[i+1]['latitude'], gift_list[i+1]['longitude']))
        weighted_distance = weighted_distance + (remaining_weight*distance)
    
    returning_distance = haversine((gift_list[gift_size-1]['latitude'], gift_list[gift_size-1]['longitude']), (NORTH_POLE_LAT, NORTH_POLE_LONG))
    weighted_distance = weighted_distance + (EMPTY_SLEIGH_WEIGHT*returning_distance)
    
    return weighted_distance

# mutate gift list
def mutateGiftList(gift_list): 
    
    i,j = generateSwapIndices(len(gift_list))
    temp = gift_list[i]
    gift_list[i] = gift_list[j]
    gift_list[j] = temp
    
    return gift_list

# pick 2 indices from pool selection
def generateSwapIndices(max_size):
    
    a = random.randint(0,max_size-1)
    b = random.randint(0,max_size-1) 
    
    while b == a:
        b = random.randint(0,max_size-1)
        
    return a,b