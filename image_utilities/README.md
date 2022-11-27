# Image Utilities
This folder contains all functions necessary to load data from the ImageNet dataset and perform transformation using the Pillow library. 

## Load Images
This file loads data from the ImageNet library using Deep Lake.  The only function users should access in load_image_data.  This function accepts a Spark Session, batch size, and currect epoch as input and returns aSpark DataFrame of Pillow Images for this batch.  Since the ImageNet dataset is too large to fit in memory, we opt to stream the data in batches.  The index of images for each batch is computed using the batch size and epoch inputs.  The images are loaded from Deep Lake as tensors so they need to be converted to byte objects to create a Spark DataFrame.  Once loaded to the DataFrame as byte objects, they are finally converted to Pillow Image objects and returned. 

## Image Transformations
This file contains image transformation functions that are wrapped as OptexProcesses.  Each method transforms all images in the DataFrame using map and Pillow functions.  

### Resize Image
This function resizes as image to the given width and height.  It accepts the DataFrame to be transformed, the new width in pixels, and new height in pixel and returns the transformed DataFrame. This function uses the Pillow resize method.

### Rotate Image
This function rotates as image to the given angle.  It accepts the DataFrame to be transformed and the new angle as a whole number of degrees,and returns the transformed DataFrame. This function uses the Pillow rotate method.

### Blur Image
This function blurs as image.  It accepts the DataFrame to be transformed and returns the transformed DataFrame. This function uses the Pillow blur method.

### Recolor Image
This function recolors as image using the CMYK color model.  It accepts the DataFrame to be transformed and returns the transformed DataFrame. This function uses the Pillow recolor method.