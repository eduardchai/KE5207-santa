import redis
import json

REDIS_CLIENT = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_individual_trip_list(ind_id):
    return json.loads(REDIS_CLIENT.hget(f'ind-{ind_id}', 'trip_list'))

def get_individual_total_cost(ind_id):
    return float(REDIS_CLIENT.hget(f'ind-{ind_id}', 'total_cost'))

if __name__ == "__main__":
  trip_list = get_individual_trip_list(0)
  total_cost = get_individual_total_cost(0)

  print(len(trip_list))
  print(total_cost)