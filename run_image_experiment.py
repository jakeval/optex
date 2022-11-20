from pyspark.sql import SparkSession
from core import computation_graph, graph_merge
from image_utilities.image_transformations import *
from image_utilities.load_images import load_imagenet_data
import time


def start_spark_session():
    """Start a new Spark Session.

    Start a new Spark Session with 1 local core"""
    spark = (
        SparkSession.builder.master("local[1]").appName("optex").getOrCreate()
    )

    return spark


def get_time_elapsed(start_time):
    """Get the time elapsed from the start time until now

    Using the time library, compute the time elapsed from the
    start time argument until the current time.  This is intended
    to be used to time how long it takes nodes to execute.

    Args:
        start_time: The object representing the start time."""
    end_time = time.time()
    return end_time - start_time


class ImagePipeline1:
    def __init__(self, batch_size, number_epochs):
        self.batch_size = batch_size
        self.number_epochs = number_epochs
        self.spark_session = start_spark_session()

    def run_pipline(self):
        epoch_count = 0
        while epoch_count < self.number_epochs:
            image_df = load_imagenet_data(
                computation_graph.Artifact(self.spark_session),
                computation_graph.Artifact(self.batch_size),
                computation_graph.Artifact(epoch_count),
            )

            batch_output = ImagePipeline1.transform(
                image_df,
                computation_graph.Artifact(100),
                computation_graph.Artifact(200),
            )
            epoch_count = epoch_count + 1
        return  # batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df, width, height):
        width.name = 'width'
        height.name = 'height'
        df.name = 'input_data'
        resized_df = resize_image(
            df,
            width,
            height,
        )
        #return resized_df
        resized_df.name = "resize_out"
        rotated_df = rotate_image(
            resized_df,
            computation_graph.Artifact(100),
        )
        rotated_df.name = "rotate_out"
        blur_df = blur_image(rotated_df)
        blur_df.name = "blur_df"
        recolor_df = recolor_image(blur_df)
        recolor_df.name = "recolor_df"
        return recolor_df


class ImagePipeline2:
    def __init__(self, batch_size, number_epochs):
        self.batch_size = batch_size
        self.number_epochs = number_epochs
        self.spark_session = start_spark_session()

    def run_pipline(self):
        epoch_count = 0
        while epoch_count < self.number_epochs:
            image_df = load_imagenet_data(
                computation_graph.Artifact(self.spark_session),
                computation_graph.Artifact(self.batch_size),
                computation_graph.Artifact(epoch_count),
            )
            batch_output = ImagePipeline2.transform(
                image_df
            )
            epoch_count = epoch_count + 1
        return batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df):
        df.name = "input"
        resized_df = resize_image(
            df,
            computation_graph.Artifact(100),
            computation_graph.Artifact(200),
        )
        resized_df.name = "resize_out"
        rotated_df = rotate_image(resized_df, computation_graph.Artifact(100))
        rotated_df.name = "rotate_out"
        recolor_df = recolor_image(rotated_df)
        recolor_df.name = "recolor_df"
        resized2_df = resize_image(
            recolor_df,
            computation_graph.Artifact(200),
            computation_graph.Artifact(400),
        )
        resized2_df.name = "resize2_out"
        return resized2_df


class ImagePipeline3:
    def __init__(self, batch_size, number_epochs):
        self.batch_size = batch_size
        self.number_epochs = number_epochs
        self.spark_session = start_spark_session()

    def run_pipline(self):
        epoch_count = 0
        while epoch_count < self.number_epochs:
            image_df = load_imagenet_data(
                computation_graph.Artifact(self.spark_session),
                computation_graph.Artifact(self.batch_size),
                computation_graph.Artifact(epoch_count),
            )
            batch_output = ImagePipeline3.transform(
                image_df
            )
            epoch_count = epoch_count + 1
        return batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df):
        df.name = "input"
        resized_df = resize_image(
            df,
            computation_graph.Artifact(100),
            computation_graph.Artifact(200),
        )
        resized_df.name = "resize_out"
        rotated_df = rotate_image(resized_df, computation_graph.Artifact(100))
        rotated_df.name = "rotate_out"
        recolor_df = recolor_image(rotated_df)
        recolor_df.name = "recolor_df"
        blur_df = blur_image(recolor_df)
        blur_df.name = "blur_df"
        return blur_df


if __name__ == "__main__":

    pipeline_1_graph = computation_graph.Graph.from_process(ImagePipeline1.transform)  # generate a static graph
    pipeline_1_mergeable_g = graph_merge.make_expanded_graph_copy(pipeline_1_graph)  # remove compositions and write in edge-list format
    print("The (pretty-printed) edge list for Pipeline 1 is:")
    print ([(parent.name, child.name) for role, parent, child in pipeline_1_mergeable_g.edges])

    pipeline_2_graph = computation_graph.Graph.from_process(ImagePipeline2.transform)  # generate a static graph
    pipeline_2_mergeable_g = graph_merge.make_expanded_graph_copy(pipeline_2_graph)  # remove compositions and write in edge-list format
    print("The (pretty-printed) edge list for Pipeline 2 is:")
    print ([(parent.name, child.name) for role, parent, child in pipeline_2_mergeable_g.edges])

    pipeline_3_graph = computation_graph.Graph.from_process(ImagePipeline3.transform)  # generate a static graph
    pipeline_3_mergeable_g = graph_merge.make_expanded_graph_copy(pipeline_3_graph)  # remove compositions and write in edge-list format
    print("The (pretty-printed) edge list for Pipeline 3 is:")
    print ([(parent.name, child.name) for role, parent, child in pipeline_3_mergeable_g.edges])

    # insert merging code here

    # benchmarks without merging
    start_time_img_pipeline_1 = time.time()
    img_pipeline_1 = ImagePipeline1(200,3).run_pipline()
    total_time_img_pipeline_1 = get_time_elapsed(start_time_img_pipeline_1)
    print(
        "Total time for Pipeline 1 without merging: ",
        total_time_img_pipeline_1,
        ' seconds',
    )
    start_time_img_pipeline_2 = time.time()
    img_pipeline_2 = ImagePipeline2(200, 3).run_pipline()
    total_time_img_pipeline_2 = get_time_elapsed(start_time_img_pipeline_2)
    print(
        "Total time for Pipeline 2 without merging: ",
        total_time_img_pipeline_2,
        ' seconds',
    )
    start_time_img_pipeline_3 = time.time()
    img_pipeline_3 = ImagePipeline1(200, 3).run_pipline()
    total_time_img_pipeline_3 = get_time_elapsed(start_time_img_pipeline_3)
    print(
        "Total time for Pipeline 3 without merging: ",
        total_time_img_pipeline_3,
        ' seconds',
    )
