from scipy.ndimage.filters import convolve, convolve1d
import collections
import numpy as np
import cv2
import time
import math
import matplotlib.pyplot as plt

img1 = cv2.imread('/Users/gc/Documents/image1.bmp', 0)

def masc_gaus_1d(variance, size):
    width = size//2
    dx = 1
    x = np.arange(-width, width)
    kernel_1d = np.exp(-(x ** 2) / (2 * variance ** 2))
    kernel_1d = kernel_1d / (math.sqrt(2 * np.pi) * variance)
    
    return kernel_1d

def masc_gaus_2d(variance, size):
    width = size//2
    dx = 1
    dy = 1
    x = np.arange(-width, width)
    y = np.arange(-width, width)
    x2d, y2d = np.meshgrid(x, y)
    kernel_2d = np.exp(-(x2d ** 2 + y2d ** 2) / (2 * variance ** 2))
    kernel_2d = kernel_2d / (2 * np.pi * variance ** 2)
    
    return kernel_2d

computeDirection1 = [("masc_gaus_1d", 0), ("conv1D", 1), ("masc_gaus_2d", 2), ("conv2D", 3)]
results = []

variance = 5
size = 11

start_time1 = time.time()

kernel1D = masc_gaus_1d(variance, size)

results.append((kernel1D, ["masc_gaus_1d"]))

print("--- %s seconds for Gaussian 1D transformation ---" % (time.time() - start_time1))


start_time2 = time.time()

t2_kernel1D = kernel1D[:, None]
t_kernel1D = t2_kernel1D.T
k_kernel2D = t2_kernel1D * t_kernel1D


img_convolved_1d1 = convolve(img1, k_kernel2D)
plt.figure()
plt.imshow(img_convolved_1d1, cmap = plt.cm.gray)

img_convolved_1d = convolve1d(img1, kernel1D)
plt.figure()
plt.imshow(img_convolved_1d, cmap = plt.cm.gray)

results.append((img_convolved_1d, ["masc_gaus_1d", "img_convolved_1d"]))

print("--- %s seconds for 1D and 2D Convolutions with Gaussian 1D transforms ---" % (time.time() - start_time2))

start_time3 = time.time()

kernel = masc_gaus_2d(variance, size)

results.append((kernel, ["masc_gaus_2d"]))

print("--- %s seconds for Gaussian 2D transformation ---" % (time.time() - start_time3))

start_time4 = time.time()

img_convolved = convolve(img1, kernel)
plt.figure()
plt.imshow(img_convolved, cmap = plt.cm.gray)

results.append((img_convolved, ["masc_gaus_2d", "img_convolved"]))

print("--- %s seconds for 1D and 2D Convolutions with Gaussian 2D transforms ---" % (time.time() - start_time4))

print(results)

##execution of a second lineage graph 
computeDirection2 = [("masc_gaus_1d", 0), ("conv1D", 1)]
results2 = []

checkGraphLineage = ["masc_gaus_1d", "img_convolved_1d"]

start_time5 = time.time()

for result in results:
    if collections.Counter(result[1]) == collections.Counter(checkGraphLineage):
        print("found")
        results2.append((result[0], checkGraphLineage))
        
print(results2)

print("--- %s seconds for lookup of lineage ---" % (time.time() - start_time5))

