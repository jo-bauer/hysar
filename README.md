
This is the source code for the paper "Hybrid Session-aware Recommendation with Feature-based Models" authored by Josef Bauer and Dietmar Jannach (University of Klagenfurt, Austria).

The setup requires both R and Python with various packages to be installed, as can be seen in the code.

The gbmrec_main.r file can be used to run all individual parts of the code with their description and respective execution order.

The raw data should be put in a /data/dataset_name/raw/ folder, on which the preprocessing is applied.

In the /code/py/first_phase_algorithms/ directory, the code for the first phase recommenders can be found, in particular gru4rec by Balázs Hidasi and s_sknn by Malte Ludewig (see the github.com/hidasib/GRU4Rec and github.com/rn5l/session-rec repos, as well as references to the corresponding articles in the paper).
The run_first_phase_recommender.py is an extension of code by Balázs Hidasi and Malte Ludewig and executes these algorithms in a step-wise manner and stores the results in the required format for the proposed method.
The proposed HySAR method consists of different submodules and performs an extensive generation of helpful features and stacks the predictions of the individual session-based recommenders with these features in one or several GBM models to improve recommendations.
More details can be found in the paper.


______________________________
 josef.b.bauer at gmail.com
