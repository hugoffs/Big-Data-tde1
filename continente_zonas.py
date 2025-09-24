#!/usr/bin/env python3
# python continente_zonas.py zones.csv --output resultadoContinentes
from mrjob.job import MRJob
from collections import Counter
import csv

class ContinentExtremes(MRJob):
    CONTINENT_INDEX = 1  # coluna "Continent" no CSV

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        
        if len(row) <= self.CONTINENT_INDEX:
            return

        continente = (row[self.CONTINENT_INDEX] or "").strip()

        
        if not continente or continente.lower() in {"continent"}:
            return

        yield "continente", continente

    def reducer(self, key, values):
        contagem = Counter(values)
        if not contagem:
            return

        max_count = max(contagem.values())
        min_count = min(contagem.values())

        cont_max = sorted([c for c, n in contagem.items() if n == max_count])
        cont_min = sorted([c for c, n in contagem.items() if n == min_count])

        # Formata saída
        yield "Continente(s) mais comum(ns)", {"Continentes": cont_max, "Numero": max_count}
        yield "Continente(s) menos comum(ns)", {"Continentes": cont_min, "Numero": min_count}

if __name__ == "__main__":
    ContinentExtremes.run()
