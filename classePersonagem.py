# pega a classe e faz a soma das quantidade de jogadores por cada clase 
#python .\classePersonagem.py .\wowah_data.csv --output resultadoClasse
from mrjob.job import MRJob

class classePersonagem(MRJob):

    def mapper(self,_,line):
        comluns = line.strip().split(",")
        classe = comluns[3].strip()
        yield classe, 1

    def reducer(self, key, counts):
        yield key,  sum(counts)
    

if __name__ == "__main__":
    classePersonagem.run()    