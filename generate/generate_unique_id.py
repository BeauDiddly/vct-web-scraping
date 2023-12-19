import random

def generate_unique_id(existing_ids):
    while True:
        new_id = random.randint(100000, 999999)
        if new_id not in existing_ids:
            existing_ids.add(new_id)
            return new_id