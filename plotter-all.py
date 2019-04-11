import matplotlib.pyplot as plt
import sys
import numpy as np

colours = ["black","red","green","blue", "cyan", "magenta"]
def parse(filename):
    result = []
    with open(filename) as fp:
        for cnt, line in enumerate(fp):
            if line.startswith('>>'):
                word_list = line.split()
                result.append(int(word_list[-1]))
    # print("Printing result: ", result)
    return np.array(result)

iterator = iter(sys.argv[1:])
coloursIter = iter(colours)
for arg in iterator:
	filePath = arg
	name = next(iterator)
	print (filePath, name)

	points = parse(filePath);
	print (points)
	plt.plot(points, next(coloursIter), label=name)

plt.legend(loc='best')
plt.show()