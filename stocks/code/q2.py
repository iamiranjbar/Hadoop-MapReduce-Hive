from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol

class MostTraded(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        name = columns[0]
        try:
            volume = float(columns[3])
            yield None, (name, volume)
        except ValueError:
            pass

    def reducer(self, key, volume):
        sorted_by_volume = sorted(volume, key = lambda x: x[1], reverse=True)
        
        for i in range(1, 11):
            yield None, sorted_by_volume[i]


if __name__ == '__main__':
    MostTraded.run()
