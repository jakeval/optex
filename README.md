# optex
Optex **opt**imizes and **ex**plains multiple overlapping data preprocessing workflows.

optex is developed as a final project for the class COMPSCI 532 Systems for Data Science at University of Massachusetts, Amherst. Optex is a toolkit for building data science preprocessing pipelines in Apache Spark as an interpretable computation graph. The computation graph is a useful interactive explanation tool and is compatible with the [Open Provenance Model](https://www.sciencedirect.com/science/article/abs/pii/S0167739X10001275). The computation graph is also analyzed and merged with other, overlapping computation graphs built from different pipelines. Merging overlapping computation graphs prevents redundant computation on batched workloads.

## How to use this library
After cloning the repo, make sure that you have installed all of the dependencies using `pip install -r requirements.txt` in the main directory.

The main infrustructure for this library consists in the core folder.  All objects and decorators needed to create an optex pipeline can be found in this folder.  See the README in the core folder for details about how to use each of the features. 

For testing, we use the ImageNet dataset.  For this reason, we have created the image_utilities folder which contains functions to load data from the ImageNet dataset using Deep Lake. This images are transformed into Pillow Image objects that are accessible as a Spark DataFrame.  There are also functions for transforming Pillow Images in this folder. 

Finally, the run_image_experiment file in the main directory can be used as an example for setting up pipelines.  Note that you will want to be familiar with the information in the core directory in order to apply this demo to new scenarios.  The run_image_experiment file combines the core and image_utilities folders in order to create three seperate pipelines that are used to prove that merging pipelines increases system efficiency. 
To run the experiments, simply run the main method in run_image_experiment.  This will print the time to execute each of the pipelines
in the console.  Additionally, visualizations of each pipelines will be saved as HTML files in the main folder on your local computer.