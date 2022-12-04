from pyspark.sql import SparkSession
from core import computation_graph, graph_merge
from image_utilities.image_transformations import *
from image_utilities.load_images import load_imagenet_data
from visualize_graph.network_graph import Network_Graph
import time


def start_spark_session():
    """Start a new Spark Session.

    Start a new Spark Session with 1 local core"""
    spark = (
        SparkSession.builder.master("local[1]")
            .config("spark.executor.memory", "7g")
            .config("spark.driver.memory", "5g")
            .config("spark.memory.offHeap.enabled",True)
            .config("spark.memory.offHeap.size","6g").appName("optex").getOrCreate()
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


class PipelineRunner:
    def __init__(self, batch_size, number_batches):
        self.batch_size = batch_size
        self.number_batches = number_batches
        self.spark_session = start_spark_session()

    def run_pipelines(self, *pipelines):
        batch_index = 0
        spark_session = computation_graph.Artifact(self.spark_session)
        batch_size = computation_graph.Artifact(self.batch_size)
        execution_times = {}
        for pipeline in pipelines:
            execution_times[pipeline] = 0
        while batch_index < self.number_batches:
            batch_index_artifact = computation_graph.Artifact(batch_index)
            for pipeline in pipelines:
                start_time = time.time()
                pipeline.run_batch(
                    spark_session, batch_size, batch_index_artifact
                )
                execution_times[pipeline] += get_time_elapsed(start_time)
            batch_index = batch_index + 1
        return execution_times

    def run_graphs(self, *graphs, output_filename='merged_graphs.html'):
        batch_index = 0
        spark_session_artifact = computation_graph.Artifact(self.spark_session)
        batch_size_artifact = computation_graph.Artifact(self.batch_size)
        execution_time = 0
        while batch_index < self.number_batches:
            batch_index_artifact = computation_graph.Artifact(batch_index)

            # These are the inputs to call the unmerged graph with. They are
            # used to execute the graph or to merge it.
            inputs = graph_merge.get_inputs(
                [
                    (
                        graph,
                        load_imagenet_data,
                        "spark_session",
                        spark_session_artifact,
                    )
                    for graph in graphs
                ]
                + [
                    (
                        graph,
                        load_imagenet_data,
                        "batch_size",
                        batch_size_artifact,
                    )
                    for graph in graphs
                ]
                + [
                    (
                        graph,
                        load_imagenet_data,
                        "batch_index",
                        batch_index_artifact,
                    )
                    for graph in graphs
                ]
            )
            graph, merged_inputs, _ = graph_merge.merge_graphs(
                graphs, inputs, name="merged_graph"
            )
            Network_Graph(graph).save_graph(output_filename)
            # These are the arguments used to call the merged graph with.
            # Because the merged graph has fewer nodes, the inputs are
            # simpler.
            inputs = graph_merge.get_merged_inputs(
                merged_inputs,
                {
                    self.spark_session: (
                        graphs[0].name,
                        load_imagenet_data,
                        "spark_session",
                    ),
                    self.batch_size: (
                        graphs[0].name,
                        load_imagenet_data,
                        "batch_size",
                    ),
                    batch_index: (
                        graphs[0].name,
                        load_imagenet_data,
                        "batch_index",
                    ),
                },
            )
            start_time = time.time()
            graph_merge.execute_graph(graph, inputs)
            execution_time += get_time_elapsed(start_time)
            batch_index += 1
        return execution_time


class ImagePipeline1:
    def __init__(self, batch_size, number_batches):
        self.batch_size = batch_size
        self.number_batches = number_batches
        self.spark_session = start_spark_session()

    def run_pipline(self):
        batch_index = 0
        spark_session = computation_graph.Artifact(self.spark_session)
        batch_size = computation_graph.Artifact(self.batch_size)
        while batch_index < self.number_batches:
            batch_index_artifact = computation_graph.Artifact(batch_index)
            ImagePipeline1.run_batch(
                spark_session, batch_size, batch_index_artifact
            )
            batch_index = batch_index + 1

    @staticmethod
    @computation_graph.optex_composition("pipeline_1_return")
    def run_batch(spark_session, batch_size, batch_index):
        spark_session.name = "spark_session"
        batch_size.name = "batch_size"
        batch_index.name = "batch_index"
        image_df = load_imagenet_data(spark_session, batch_size, batch_index)
        batch_output = ImagePipeline1.transform(image_df)
        return batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df):
        df.name = "input_data"
        resized_df = resize_image(df)
        # return resized_df
        resized_df.name = "resize_out"
        rotated_df = rotate_image(resized_df)
        rotated_df.name = "rotate_out"
        blur_df = blur_image(rotated_df)
        blur_df.name = "blur_df"
        recolor_df = recolor_image(blur_df)
        recolor_df.name = "recolor_df"
        return collect(recolor_df)


class ImagePipeline2:
    def __init__(self, batch_size, number_batches):
        self.batch_size = batch_size
        self.number_batches = number_batches
        self.spark_session = start_spark_session()

    def run_pipline(self):
        batch_index = 0
        spark_session = computation_graph.Artifact(self.spark_session)
        batch_size = computation_graph.Artifact(self.batch_size)
        while batch_index < self.number_batches:
            batch_index_artifact = computation_graph.Artifact(batch_index)
            ImagePipeline2.run_batch(
                spark_session, batch_size, batch_index_artifact
            )
            batch_index = batch_index + 1

    @staticmethod
    @computation_graph.optex_composition("pipeline_2_return")
    def run_batch(spark_session, batch_size, batch_index):
        spark_session.name = "spark_session"
        batch_size.name = "batch_size"
        batch_index.name = "batch_index"
        image_df = load_imagenet_data(spark_session, batch_size, batch_index)
        batch_output = ImagePipeline2.transform(image_df)
        return batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df):
        df.name = "input"
        resized_df = resize_image(df)
        resized_df.name = "resize_out"
        rotated_df = rotate_image(resized_df)
        rotated_df.name = "rotate_out"
        recolor_df = recolor_image(rotated_df)
        recolor_df.name = "recolor_df"
        resized2_df = resize_image(recolor_df)
        resized2_df.name = "resize2_out"
        return collect(resized2_df)


class ImagePipeline3:
    def __init__(self, batch_size, number_batches):
        self.batch_size = batch_size
        self.number_batches = number_batches
        self.spark_session = start_spark_session()

    def run_pipline(self):
        batch_index = 0
        spark_session = computation_graph.Artifact(self.spark_session)
        batch_size = computation_graph.Artifact(self.batch_size)
        while batch_index < self.number_batches:
            batch_index_artifact = computation_graph.Artifact(batch_index)
            ImagePipeline3.run_batch(
                spark_session, batch_size, batch_index_artifact
            )
            batch_index = batch_index + 1

    @staticmethod
    @computation_graph.optex_composition("pipeline_3_return")
    def run_batch(spark_session, batch_size, batch_index):
        spark_session.name = "spark_session"
        batch_size.name = "batch_size"
        batch_index.name = "batch_index"
        image_df = load_imagenet_data(spark_session, batch_size, batch_index)
        batch_output = ImagePipeline3.transform(image_df)
        return batch_output

    @staticmethod
    @computation_graph.optex_composition("transform_return")
    def transform(df):
        df.name = "input"
        rotated_df = rotate_image(df)
        rotated_df.name = "rotate_out"
        recolor_df = recolor_image(rotated_df)
        recolor_df.name = "recolor_df"
        blur_df = blur_image(recolor_df)
        blur_df.name = "blur_df"
        resized_df = resize_image(blur_df)
        resized_df.name = "resize_out"
        return collect(blur_df)


if __name__ == "__main__":

    batch_size = 4 #should not exceed 200
    number_batches = 3
    runner = PipelineRunner(batch_size, number_batches)

    print("START UNMERGED")
    unmerged_execution_times = runner.run_pipelines(ImagePipeline1, ImagePipeline2, ImagePipeline3)
    print("DONE")
    print(
        "Total time for Pipeline 1 without merging: ",
        unmerged_execution_times[ImagePipeline1],
        " seconds",
    )
    print(
        "Total time for Pipeline 2 without merging: ",
        unmerged_execution_times[ImagePipeline2],
        " seconds",
    )
    print(
        "Total time for Pipeline 3 without merging: ",
        unmerged_execution_times[ImagePipeline3],
        " seconds",
    )

    pipeline_1_graph = graph_merge.make_expanded_graph_copy(
        computation_graph.Graph.from_process(
            ImagePipeline1.run_batch, "pipeline_1"
        )
    )
    pipeline_2_graph = graph_merge.make_expanded_graph_copy(
        computation_graph.Graph.from_process(
            ImagePipeline2.run_batch, "pipeline_2"
        )
    )
    pipeline_3_graph = graph_merge.make_expanded_graph_copy(
        computation_graph.Graph.from_process(
            ImagePipeline3.run_batch, "pipeline_3"
        )
    )

    print("RUN MERGED")
    merged_execution_time = runner.run_graphs(pipeline_1_graph, pipeline_2_graph, pipeline_3_graph, output_filename='merge_3_pipelines.html')
    print("DONE!")
    print(
        "Total time for all three merged pipelines: ",
        merged_execution_time,
        " seconds",
    )

    #create visuals for each individual pipeline
    pipeline_1_mergeable_g = graph_merge.make_expanded_graph_copy(
        pipeline_1_graph
    )  # remove compositions and write in edge-list format
    pipeline_1_graph_visual = Network_Graph(pipeline_1_mergeable_g)
    pipeline_1_graph_visual.save_graph("pipeline_1.html")

    pipeline_2_mergeable_g = graph_merge.make_expanded_graph_copy(
        pipeline_2_graph
    )  # remove compositions and write in edge-list format
    pipeline_2_graph_visual = Network_Graph(pipeline_2_mergeable_g)
    pipeline_2_graph_visual.save_graph("pipeline_2.html")

    pipeline_3_mergeable_g = graph_merge.make_expanded_graph_copy(
        pipeline_3_graph
    )  # remove compositions and write in edge-list format
    pipeline_3_graph_visual = Network_Graph(pipeline_3_mergeable_g)
    pipeline_3_graph_visual.save_graph("pipeline_3.html")
