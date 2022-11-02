# optex
Optex **opt**imizes and **ex**plains multiple overlapping data preprocessing workflows.

optex is developed as a final project for the class COMPSCI 532 Systems for Data Science at University of Massachusetts, Amherst. Optex is a toolkit for building data science preprocessing pipelines in Apache Spark as an interpretable computation graph. The computation graph is a useful interactive explanation tool and is compatible with the [Open Provenance Model](https://www.sciencedirect.com/science/article/abs/pii/S0167739X10001275). The computation graph is also analyzed and merged with other, overlapping computation graphs built from different pipelines. Merging overlapping computation graphs prevents redundant computation on batched workloads.
