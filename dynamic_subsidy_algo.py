# The idea, the strategy should dynamically adapt to the block competetion, ideally incase of high competetion, decrease the subsidy, using a decay rate function to increase it back to normal the decay constant can also be changed dynamically to further optimise the algo.


import math
import random
import time


NORMAL_SUBSIDY_RATE = 0.001  # Initial subsidy rate
BOOST_FACTOR = 0.20          # Increase tip by 20% on trigger
HIGH_COMPETITION_THRESHOLD = 3  # no. of similar transactions
DECAY_CONSTANT = 0.2         # The decay rate (higher = faster return to normal) likely keeping it low


current_subsidy_rate = NORMAL_SUBSIDY_RATE
time_since_competition = 0  


def check_for_similar_transactions(block_number):
    """
    Simulates checking a block for a high-  competition event.
    In a real-world scenario, this would analyze real block data.
    Here, we'll use a random chance to trigger the event.
    """
    # 10% chance of a high-competition event
    if random.random() < 0.10:
        return random.randint(HIGH_COMPETITION_THRESHOLD + 1, 10)
    else:
        return random.randint(0, HIGH_COMPETITION_THRESHOLD)

print("--- Dynamic Subsidy Algorithm Simulation ---")
print(f"Normal Subsidy Rate: {NORMAL_SUBSIDY_RATE:.4f}")
print("Starting simulation...")
print("-" * 40)


def calculate_decayed_subsidy(initial_boosted_rate, time_elapsed, decay_rate):
    """
    A(t) = A₀ * e^(-λt)
    """
    decay_factor = math.exp(-decay_rate * time_elapsed)
    calculated_rate = initial_boosted_rate * decay_factor
    
    return max(calculated_rate, NORMAL_SUBSIDY_RATE)


for block_number in range(1, 21):
    similar_tx_count = check_for_similar_transactions(block_number)

    # Check for the competition trigger
    if similar_tx_count > HIGH_COMPETITION_THRESHOLD:
        print(f"Block {block_number}: ⚠️ High competition detected! ({similar_tx_count} similar TXs)")
        time_since_competition = 0
        initial_boosted_rate = NORMAL_SUBSIDY_RATE * (1 + BOOST_FACTOR)
        current_subsidy_rate = initial_boosted_rate
    
    # Otherwise, if we're in a decay period, continue to decay
    elif time_since_competition > 0:
        initial_boosted_rate = NORMAL_SUBSIDY_RATE * (1 + BOOST_FACTOR)
        current_subsidy_rate = calculate_decayed_subsidy(
            initial_boosted_rate,
            time_since_competition,
            DECAY_CONSTANT
        )
        
        # Reset the timer if the subsidy is back to normal
        if current_subsidy_rate <= NORMAL_SUBSIDY_RATE:
            time_since_competition = 0
            
    # If no competition is active, the subsidy remains at the normal rate
    else:
        current_subsidy_rate = NORMAL_SUBSIDY_RATE
        
    print(f"Block {block_number}: Current Subsidy Rate = {current_subsidy_rate:.4f}")
    
    # Increment the time counter for the next block
    time_since_competition += 1
    
    # Pause to make the simulation readable
    time.sleep(0.5)

print("-" * 40)
print("Simulation complete.")
