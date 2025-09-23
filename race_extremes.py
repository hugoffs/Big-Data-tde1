
#!/usr/bin/env python3
# python raca_extremes.py wowah_data.csv --output resultado
from mrjob.job import MRJob
from collections import Counter
import csv

class RaceExtremes(MRJob):
    RACE_INDEX = 2 

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        # Verifica se a linha tem a coluna esperada
        if len(row) <= self.RACE_INDEX:
            return

        raca = (row[self.RACE_INDEX] or "").strip()

        if not raca or raca.lower() in {"raca", "raça", "race"}:
            return

        yield "raca", raca

    def reducer(self, key, values):
        contagem = Counter(values)
        if not contagem:
            return

        max_count = max(contagem.values())
        min_count = min(contagem.values())

        racas_max = sorted([r for r, c in contagem.items() if c == max_count])
        racas_min = sorted([r for r, c in contagem.items() if c == min_count])

        yield "Raça(s) mais comum(ns)", {"count": max_count, "racas": racas_max}
        yield "Raça(s) menos comum(ns)", {"count": min_count, "racas": racas_min}

if __name__ == "__main__":
    RaceExtremes.run()
