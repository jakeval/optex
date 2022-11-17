from pyspark.sql import SparkSession
from core import computation_graph
from image_utilities.image_transformations import ImageTransformation
from image_utilities.load_images import load_imagenet_data
import time

def start_spark_session():
    """Start a new Spark Session.

        Start a new Spark Session with 1 local core"""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("optex") \
        .getOrCreate()

    return spark

def get_time_elapsed(start_time):
    """Get the time elapsed from the start time until now

        Using the time library, compute the time elapsed from the
        start time argument until the current time.  This is intended
        to be used to time how long it takes nodes to execute.

        Args:
            start_time: The object representing the start time."""
    end_time = time.time()
    return end_time-start_time

class ImagePipeline1:
    def __init__(self, batch_size, number_epochs):
        self.batch_size = batch_size
        self.number_epochs = number_epochs
        self.spark_session = start_spark_session()

    def run_pipline(self):
        epoch_count = 0
        while epoch_count < self.number_epochs:
            image_df = load_imagenet_data(computation_graph.Artifact(self.spark_session), computation_graph.Artifact(self.batch_size), computation_graph.Artifact(epoch_count))
            batch_output = ImagePipeline1.transform(image_df)
            epoch_count = epoch_count + 1
        return batch_output

    @staticmethod
    @computation_graph.optex_composition('transform_return')
    def transform(df):
        start_resize_time = time.time()
        resized_df = ImageTransformation.resize_image(df, computation_graph.Artifact(100), computation_graph.Artifact(200))
        resized_df.name = 'resize_out'
        total_resize_time = get_time_elapsed(start_resize_time)
        rotated_df = ImageTransformation.rotate_image(resized_df, computation_graph.Artifact(100))
        rotated_df.name = 'rotate_out'
        blur_df = ImageTransformation.blur_image(rotated_df)
        blur_df.name = 'blur_df'
        recolor_df = ImageTransformation.recolor_image(rotated_df)
        recolor_df.name = 'recolor_df'
        return recolor_df

if __name__ == '__main__':
    #update the batch size and number of epochs for each experiment
    img_pipeline = ImagePipeline1(200, 3)
    computation_graph.generate_static_graph(img_pipeline.run_pipline)