from haversine import haversine

NORTH_POLE_LAT = 90
NORTH_POLE_LONG = 0
EMPTY_SLEIGH_WEIGHT = 10
SLEIGH_CAPACITY = 1000

# to calculate the cumulative weight of gifts
def tripWeightUtil(gift_list, start_index, end_index):
    
    total_weight = 0 
    
    while start_index <= end_index:
        total_weight = total_weight + gift_list[start_index].weight
        start_index = start_index + 1
    
    return total_weight 

# to calculate the cost of the trip
def tripCost(gift_list):
    
    gift_size = len(gift_list)
    initial_gift_weight = tripWeightUtil(gift_list,0,gift_size-1) + EMPTY_SLEIGH_WEIGHT
    weighted_distance = initial_gift_weight * haversine((NORTH_POLE_LAT, NORTH_POLE_LONG), (gift_list[0].latitude, gift_list[0].longitude))
    
    for i in range(gift_size-1):
        remaining_weight = tripWeightUtil(gift_list, i+1, gift_size-1) + EMPTY_SLEIGH_WEIGHT
        distance = haversine((gift_list[i].latitude, gift_list[i].longitude), (gift_list[i+1].latitude, gift_list[i+1].longitude))
        weighted_distance = weighted_distance + (remaining_weight*distance)
    
    returning_distance = haversine((gift_list[gift_size-1].latitude, gift_list[gift_size-1].longitude), (NORTH_POLE_LAT, NORTH_POLE_LONG))
    weighted_distance = weighted_distance + (EMPTY_SLEIGH_WEIGHT*returning_distance)
    
    return weighted_distance 