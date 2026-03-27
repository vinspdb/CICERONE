import pandas as pd
import numpy as np
class TrainTestBuilder():
    def __init__(self, noise, pickup_idx, all_timestamps, active_pickups):
        self.noise = noise
        self.num_vp_obj = 2000
        self.pickup_idx = pickup_idx
        self.index_train = 1400
        self.all_timestamps = all_timestamps
        self.step_size = 100
        self.pd_active_pickups = pd.DataFrame(active_pickups)
        self.pd_active_pickups.sort_values(by = 2, inplace = True)

    def sample_equally(self, input_list, num_samples):
        # Handle edge cases
        if num_samples <= 0:
            return []
        if num_samples >= len(input_list):
            return input_list

        # Calculate the interval between samples
        step = int((len(input_list) - 1) / (num_samples - 1))
        sampled_list = [input_list[i * step] for i in range(num_samples)]

        return sampled_list

    def timestamps_generator(self):

        split_timestamp = self.pd_active_pickups.iloc[self.index_train,2]
        last_timestamp = max(self.pd_active_pickups.iloc[:self.index_train,2])

        train_pickups = self.pd_active_pickups[self.pd_active_pickups[2] <= split_timestamp][0] 
        test_pickups = self.pd_active_pickups[self.pd_active_pickups[1] > last_timestamp][0]
        index_test = self.index_train + int(len(test_pickups) * .35)
        split_timestamp_val = self.pd_active_pickups.iloc[index_test,2]

        last_timestamp_val = max(self.pd_active_pickups.iloc[self.index_train : index_test,2])

        val_pickups = self.pd_active_pickups[(self.pd_active_pickups[1] > last_timestamp) & (self.pd_active_pickups[2] <= split_timestamp_val)][0]
        test_pickups = self.pd_active_pickups[self.pd_active_pickups[1] > last_timestamp_val][0]

        train_timestamps = self.all_timestamps[np.isin(self.pickup_idx, train_pickups.values)]
        val_timestamps = self.all_timestamps[np.isin(self.pickup_idx, val_pickups.values)]
        test_timestamps = self.all_timestamps[np.isin(self.pickup_idx, test_pickups.values)]

        train_sampled_timestamps = self.sample_equally(train_timestamps, int(len(train_timestamps) / self.step_size))
        val_sampled_timestamps = self.sample_equally(val_timestamps, int(len(val_timestamps) / self.step_size))
        test_sampled_timestamps = self.sample_equally(test_timestamps, int(len(test_timestamps) / self.step_size))

        print("Generated timestamps!")

        return train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps
    

        