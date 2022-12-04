from PIL import Image
import deeplake
import torchvision.transforms as T
from core import computation_graph
import numpy as np
from pyspark.sql import Row


def convert_spark_to_pil(img):
    """Convert a Spark byte object to a Pillow Image object.

    A byte object can be converted to a Pillow object, assuming an RGBA or
    RGB color model. The images by default have a blue tint but is
    resolved by transforming the byte array before creating the Pillow
    Image object.

    Args:
        img: The byte object representing an image."""
    # mode = "RGBA" if (img.image.nChannels == 4) else "RGB"
    mode = "RGB"
    image = Image.frombytes(
        mode=mode,
        data=bytes(img),
        size=[200, 200],
    )
    # fix blue tint
    B, G, R = np.asarray(image).T
    converted_img_array = np.array((R, G, B)).T
    return Image.fromarray(converted_img_array)


@computation_graph.optex_process("data_return")
def load_imagenet_data(spark_session, batch_size, batch_index):
    """Load ImageNet data for a batch using Deep Lake.

    Based on the batch size and current epoch, load the image for this batch.
    Images from Deep Lake are returned as tensors so each tensor image must be
    converted to a Pillow Image. Spark requires primitive types to create dataframes
    so we convert the Pillow Image to a byte object, then use the byte object to
    create a Spark dataframe.  Finally, we can convert the Spark dataframe from byte
    objects to Pillow objects.

    Args:
        spark_session: The active Spark session.
        batch_size: A number representing the batch size.
        batch_index: A number representing the index of the batch to retrieve."""

    # load dataset from deep lake
    # you MUST include a valid token to access the data
    # to get a valid token, you must create a deep lake account
    ds = deeplake.load(
        "hub://activeloop/imagenet-val",
        token="eyJhbGciOiJIUzUxMiIsImlhdCI6MTY2NzE0NTM3OCwiZXhwIjoxNjcxNTU1MzAwfQ.eyJpZCI6ImNiYXNpbGllcmUifQ.PiuT0jl1U9n8JgzrMCmCvsLxN4BXtQoJJzVHSgWOHLrNKmyKcSJhRjOpoNlqc2Jc2nharFq6D667n7IHymLtAA",
    )

    # define function to translate deep lake tensors to pillow images
    transform = T.ToPILImage()

    # for the indices for this batch, translate the tensors to byte objects representing the images
    tensor_data = []
    i = 0 + batch_size * batch_index
    last_batch_index = i + batch_size
    while i < last_batch_index:
        tensor_data.append(
            (
                ds.labels[i].data()["text"][0],
                transform(ds.images[i].numpy()).tobytes(),
            )
        )
        i = i + 1
        if i % 100 == 0:
            print(i)
    df_labels = ["label", "image"]

    # create a spark dataframe from the image data
    image_df = spark_session.createDataFrame(
        data=tensor_data, schema=df_labels
    )
    image_df.printSchema()

    # finally, convert spark byte image objects to pillow image objects
    converted_image_df = image_df.rdd.map(lambda x: convert_spark_to_pil(x))
    for element in converted_image_df.collect():
        print(element)

    # dummy_image = None
    # with Image.open("image_utilities/lamp.jpg") as im:
    #     dummy_image = im
    #     im.show()
    # dummy_image.show()
    # sc = spark_session.sparkContext
    # rdd = sc.parallelize([dummy_image.tobytes(), dummy_image.tobytes()])
    # row = Row('image')
    # image_df = rdd.map(row).toDF()
    # # image_df = spark_session.createDataFrame(
    # #     data=tensor_data, schema=['image']
    # # )
    # print(image_df)
    # #dummy = convert_spark_to_pil(dummy_image.tobytes())
    # #dummy.show()
    # converted_image_df = image_df.select('image').rdd.map(lambda x: convert_spark_to_pil(x)).toDF()
    # #converted_image_df = image_df.rdd.map(lambda x: x['image'])
    #
    # for ele in converted_image_df.collect():
    #     print(len(ele))


    return converted_image_df
