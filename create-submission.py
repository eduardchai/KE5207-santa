import csv
import redis
import json

REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_individual(ind_id):
    return json.loads(REDIS_CLIENT.hget(f'ind-{ind_id}', 'trip_list'))

def calculate_weight(gift_list):
    total_weight = 0
    for i in gift_list:
        weight = float(REDIS_CLIENT.hget('weight-lookup', f'gift-{i}'))
        total_weight += weight
    
    return total_weight

def create_submission(ind):
    file_path = './submission.csv'

    with open(file_path, mode="w") as trip_file:
        trip_writer = csv.writer(trip_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        trip_writer.writerow(["GiftId", "TripId"])
        trip_id = 0
        for trip in ind:
            for gift in trip:
                trip_writer.writerow([gift, trip_id])
            trip_id += 1

if __name__ == '__main__':
    ind = get_individual(1)

    # count = 0
    # for trip in ind:
    #     total_weight = calculate_weight(trip)
    #     if total_weight > 1000:
    #         print(f"trip:{count} total_weight:{total_weight}")
    #     count += 1

    create_submission(ind)