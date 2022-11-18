from core import computation_graph
from PIL import ImageFilter


class ImageTransformation:
    """A class containing all possible image transformations available for optex.

    Functions from this class can be used to transform Pillow Image objects.  Images
    must be converted to Pillow Images before using this class. This class includes
    various functions from the Pillow library and some functions require arguments."""

    @computation_graph.optex_process('resize_return')
    def resize_image(df, width, height):
        """Resizes a Pillow image to the indicated dimensions.

        Use the Pillow library to resize an image.

        Args:
            df: The dataframe containing Pillow Images.
            width: Integer representing the desired width.
            height: Integer representing the desired height."""
        return df.map(lambda img: img.resize((width, height)))

    @computation_graph.optex_process('rotate_return')
    def rotate_image(df, angle):
        """Rotates a Pillow image to the indicated degree.

        Use the Pillow library to rotate an image.

        Args:
            df: The dataframe containing Pillow Images.
            angle: Integer representing the desired angle to rotate."""
        return df.map(lambda img: img.rotate(angle=angle))

    @computation_graph.optex_process('blur_return')
    def blur_image(df):
        """Blurs a Pillow image.

        Use the Pillow library to blur an image.

        Args:
            df: The dataframe containing Pillow Images."""
        return df.map(lambda img: img.filter(ImageFilter.BLUR))

    @computation_graph.optex_process('recolor_return')
    def recolor_image(df):
        """Recolors a Pillow image using the CMYK color model.

        Use the Pillow library to recolor an image to CMYK.

        Args:
            df: The dataframe containing Pillow Images."""
        return df.map(lambda img: img.convert(mode='CMYK'))