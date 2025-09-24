#!/usr/bin/env python3
# python analise_level_guild.py wowah_data.csv --output resultado_level_guild
from mrjob.job import MRJob

class AnaliseLevelGuildSimplificado(MRJob):

    def mapper(self, _, line):
        try:
            columns = line.strip().split(",")
            if len(columns) >= 7:
                level = int(columns[1].strip())
                guild = columns[5].strip()

                yield "level", level

                if guild and guild != "-1":
                    yield f"guild_{guild}", 1
        except (ValueError, IndexError):
            pass

    def reducer(self, key, values):
        if key.startswith("guild_"):
            yield key, sum(values)
        elif key == "level":
            level_list = list(values)
            if level_list:
                min_level = min(level_list)
                max_level = max(level_list)
                avg_level = sum(level_list) / len(level_list)
                yield "min_level", min_level
                yield "max_level", max_level
                yield "avg_level", avg_level

if __name__ == '__main__':
    AnaliseLevelGuildSimplificado.run()