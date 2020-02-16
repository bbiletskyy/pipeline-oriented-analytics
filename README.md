# pipeline-oriented-analytics

This is a tutorial demonstrating pipeline-oriented data analytics approach applied to taxi trip duration data.
This project should NOT be viewed as an example how to solve a particular regression problem. 
It is rather a demonstration how to organize code in data analytics projects. 
For example, some features were introduced artificially just for demo purposes. 

## Prerequisites

* [Anaconda](https://www.continuum.io/downloads)
* [JDK 8](https://docs.oracle.com/javase/8/docs/technotes/guides/install/linux_jdk.html)

## Getting started
 
1. Run `make init test` to initialize the conda environment and to launch the tests
2. (Optional, sample datasets are available) Download complete train and test datasets from [Kaggle's New York City Trip Duration](https://www.kaggle.com/c/nyc-taxi-trip-duration/data), extract them and overwrite `train.csv`, `test.csv` in `data/raw` folder.

## Running examples

1. Execute `make distance_matrix` to generate distance matrix
2. Execute `make prepare_train features_train train` to pre-process train data, extract train features and train
3. Execute `make prepare_test features_test predict` to pre-process test data, extract test features and predict
4. Execute `make select_params` to run hyper-parameter tuning.



