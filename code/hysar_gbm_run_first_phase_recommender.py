
# Code for generating the predictions by the first phase recommenders.
# It is based on an adaption of code for a rolling evaluation by Balázs Hidasi and Malte Ludewig.
# For the specified first phase recommendation method and the number of time steps (buckets),
# a dataset with predictions is generated which can in turn be used by the second phase GBM method.

# """
# @author: Balázs Hidasi
# @author: mludewig
# @author: Josef Bauer
# """


import time
import numpy as np
import json
import pickle
import codecs
import gc
import sys
import pandas as pd
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description='Run first phase recommender.')
parser.add_argument('--base_path')
parser.add_argument('--dataset')
parser.add_argument('--method')
parser.add_argument('--target_case')
args = parser.parse_args()

if args.base_path is None:
    base_path = '~/hysar_gbm/'
else:
    base_path = args.base_path

sys.path.append(base_path)

if args.dataset is None:
    dataset = 'diginetica'
else:
    dataset = args.dataset

if args.target_case is None:
    target_case = 'buys'
else:
    target_case = args.target_case

if args.method is None:
    method = 's_sknn'
else:
    method = args.method


module_path = base_path + 'code' + '/'
sys.path.append(module_path)

from first_phase_algorithms import s_sknn as s_sknn
from first_phase_algorithms import gru4rec as gru4rec

file_suffix = '_' + target_case
file_prefix = 'events' + file_suffix + '_' + 'prepared'

numberOfTrainingItemsPerSession = 200
number_of_top_items = numberOfTrainingItemsPerSession

# Number of time buckets
number_of_steps = 20

data_path = base_path + 'data' + '/' + dataset + '/' + 'prepared' + '/'
cache_path = base_path + 'output' + '/' + 'cache' + '/'
output_path = cache_path
output_table_file_name = 'first_phase_recommender_prediction_results_' + dataset + '_' + method + file_suffix + '.csv'


# Note that an alternative is to use the rpy2 package and to read the settings and call this code directly from R (or a json config etc.).
# It is removed here for simplification.
if 'gru4rec' in method:
    gru4rec_n_epochs = 10
    gru4rec_loss = 'bpr-max-0.5'
    gru4rec_layers = [100]
    gru4rec_batch_size = 32
    gru4rec_learning_rate = 0.2
    gru4rec_n_sample = 2048

elif method == 's_sknn':
    s_sknn_number_of_neighbor_sessions = 100
    s_sknn_sample_size = 500

else:
    print('method unknown')

train_cutoff_percentage = 0
limit_train = None

session_key = 'SessionId'
item_key = 'ItemId'
time_key = 'Time'
user_key = 'UserId'

split = ''
train_appendix = '_all_data'

if __name__ == '__main__':

    train_data = pd.read_csv(data_path + file_prefix + train_appendix + split + '.txt', sep='\t', dtype={'ItemId':np.int64})

    item_ids = train_data.ItemId.unique()

    train_data['SessionId'] = train_data['SessionId'].astype('int')

    min_time = train_data[time_key].min()

    max_time = train_data[time_key].max()

    if dataset == 'retailrocket':

        time_interval_list = [time for time in range(min_time, max_time + 1, int(round((max_time - min_time)/number_of_steps)))]

    if dataset == 'diginetica':

        time_interval_list = [int(round(qval)) for qval in train_data[time_key].quantile(q = np.array(np.arange(0, 1, ((1 - 0)/number_of_steps))))]

        time_interval_list.append(max_time)

    session_max_times = train_data.groupby(session_key)[time_key].max()

    session_min_times = train_data.groupby(session_key)[time_key].min()

    algs = []

    if dataset == 'diginetica':

        train_data.loc[train_data['UserId'].isna(), 'UserId'] = 0

    train_data.sort_values(['SessionId', 'Time'], inplace = True)


    for time_step in range(0, len(time_interval_list) - 2):

        gc.collect()

        print('Training first phase session-based recommender' + ' ' + method + ' ' + 'Time Step: ' + str(time_step))

        train_subset_data = train_data.loc[train_data[time_key] <= time_interval_list[time_step + 1]]

        train_subset_min_time = train_subset_data[time_key].min()

        train_subset_max_time = train_subset_data[time_key].max()

        sessions_train_subset = session_max_times[ session_max_times <= train_subset_max_time ].index

        train_subset_data = train_subset_data[np.in1d(train_subset_data[session_key], sessions_train_subset)]

        train_subset_session_lengths = train_subset_data.groupby(session_key, as_index=False).size()

        if not (isinstance(train_subset_session_lengths, pd.DataFrame)):

            train_subset_session_lengths = pd.DataFrame(train_subset_session_lengths)

            train_subset_session_lengths = train_subset_session_lengths.reset_index()

            train_subset_session_lengths.columns = [train_subset_session_lengths.columns[0], 'size']

        train_subset_data = train_subset_data.reset_index()

        train_subset_data = train_subset_data[np.in1d(train_subset_data[session_key],
                                                        train_subset_session_lengths[train_subset_session_lengths['size'] >= 2][session_key])]
        train_subset_data = train_subset_data.reset_index(drop=True)

        if 'gru4rec' in method:

            alg = gru4rec.GRU4Rec(n_epochs=gru4rec_n_epochs, loss=gru4rec_loss, final_act='linear', hidden_act='tanh',
                                    layers=gru4rec_layers, batch_size=gru4rec_batch_size, dropout_p_hidden=0.0,
                                    learning_rate=gru4rec_learning_rate, momentum=0.5, n_sample=gru4rec_n_sample, sample_alpha=0, time_sort=True)

        if 's_sknn' in method:

            alg = s_sknn.SeqContextKNN(s_sknn_number_of_neighbor_sessions, s_sknn_sample_size, similarity="cosine", extend=False)

        print(alg)

        alg.fit(train_subset_data)


        items_to_predict = train_subset_data[item_key].unique()

        new_train_subset_data = train_data.loc[(train_data[time_key] <= time_interval_list[time_step + 2]) & \
                                                (train_data[time_key] > time_interval_list[time_step + 1])]

        new_train_subset_data = new_train_subset_data[new_train_subset_data[item_key].isin(items_to_predict)]

        new_train_subset_min_time = new_train_subset_data[time_key].min()

        new_train_subset_max_time = new_train_subset_data[time_key].max()

        sessions_new_train_subset = \
            (session_max_times[session_max_times <= new_train_subset_max_time].index) \
            .intersection(session_min_times[session_min_times >= new_train_subset_min_time].index)

        new_train_subset_data = new_train_subset_data[np.in1d(new_train_subset_data[session_key], sessions_new_train_subset)]

        new_train_subset_session_lengths = new_train_subset_data.groupby(session_key, as_index=False).size()

        if not (isinstance(new_train_subset_session_lengths, pd.DataFrame)):

            new_train_subset_session_lengths = pd.DataFrame(new_train_subset_session_lengths)

            new_train_subset_session_lengths = new_train_subset_session_lengths.reset_index()

            new_train_subset_session_lengths.columns = [new_train_subset_session_lengths.columns[0], 'size']

        new_train_subset_data = new_train_subset_data.reset_index()

        new_train_subset_data = \
            new_train_subset_data[np.in1d(new_train_subset_data[session_key],
                                            new_train_subset_session_lengths[new_train_subset_session_lengths['size'] >= 2][session_key])]

        new_train_subset_data = new_train_subset_data.reset_index(drop=True)

        offset_sessions = np.zeros(new_train_subset_data[session_key].nunique() + 1, dtype=np.int32)

        length_session = np.zeros(new_train_subset_data[session_key].nunique(), dtype=np.int32)

        offset_sessions[1:] = new_train_subset_data.groupby(session_key).size().cumsum()

        length_session[0:] = new_train_subset_data.groupby(session_key).size()

        current_session_idx = 0

        pos = offset_sessions[current_session_idx]

        finished = False

        count = 0

        while not finished:

            current_item = new_train_subset_data[item_key][pos]

            current_session = new_train_subset_data[session_key][pos]

            current_user = new_train_subset_data[user_key][pos]

            ts = new_train_subset_data[time_key][pos]

            rest = new_train_subset_data[item_key][pos+1:offset_sessions[current_session_idx] + length_session[current_session_idx]].values

            preds = alg.predict_next(current_session, current_item, items_to_predict, timestamp=ts)

            preds[np.isnan(preds)] = 0

            preds.sort_values(ascending=False, inplace=True)

            export_df_temp = \
                pd.DataFrame({
                        "current_item_id": current_item
                       ,"future_item_id": preds[:number_of_top_items].index
                       ,"session_id": current_session
                       ,"timestamp_int": ts
                       ,"time_step": time_step
                       ,"preds": preds[:number_of_top_items]
                    })

            export_df_temp.reset_index(drop = True, inplace = True)

            if count == 0:

                export_df = export_df_temp.copy()

            else:

                export_df = pd.concat([export_df, export_df_temp], axis = 0)

            count += 1

            pos += 1

            if pos + 1 == offset_sessions[current_session_idx] + length_session[current_session_idx]:

                current_session_idx += 1

                if current_session_idx == new_train_subset_data[session_key].nunique():

                    finished = True

                pos = offset_sessions[current_session_idx]

                count += 1

            if finished == True and time_step == 0:

                        mode_val = 'w'

                        header_val = True

            else:

                        mode_val = 'a+'

                        header_val = False

            if finished == True:

                export_df.to_csv(output_path + output_table_file_name, sep = ";",
                                      mode = mode_val, header = header_val, index = False)

        print('Finished writing data for Time Step' + ' ' + str(time_step))


#______________________________
# josef.b.bauer at gmail.com
