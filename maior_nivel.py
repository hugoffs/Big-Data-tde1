#pega o maior e menor nivel, junto com a quantidade de jogadores que estão nos nivel.

from mrjob.job import MRJob
import numpy as np

class MRMaxLevel(MRJob):
    def mapper(self, _, line):
        columns = line.split(",")
        level = int(columns[1].strip())
        key = "Level:"
        yield key, (level, 1)


    def reducer(self, key, values):
        min_level = np.inf
        max_level = -np.inf
        count_max = 0
        count_min = 0 
        for level, n in values:
            if level > max_level:
                max_level = level
                count_max = n
            elif level == max_level:
                count_max += n

            if level < min_level:
                min_level = level
                count_min
            elif level == min_level:
                count_min += n

        result = {}
        result["Maior Level:"] = max_level
        result["Numero Jogadores com Maior level:"] = count_max
        result["Menor Level:"] = min_level
        result["Numero jogadores com menor Level:"] = count_min
        for k in result:
            yield k, result[k]

if __name__ == "__main__":
    MRMaxLevel.run()