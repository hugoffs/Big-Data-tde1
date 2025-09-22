from mrjob.job import MRJob
import numpy as np

class MRMaxLevel(MRJob):
    def mapper(self, _, line):
        columns = line.split(",")
        level = int(columns[1].strip())
        yield "max_level", (level, 1)


    def reducer(self, key, values):
        max_level = -np.inf
        count = 0
        for level, n in values:
            if level > max_level:
                max_level = level
                count = n
            elif level == max_level:
                count += n
        yield "max_level", (max_level, count)

if __name__ == "__main__":
    MRMaxLevel.run()