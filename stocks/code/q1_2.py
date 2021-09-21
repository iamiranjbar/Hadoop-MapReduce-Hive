from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol

class Cheapest(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        yield None,(columns[0], columns[10])

    def reducer(self, key, price):
        price = sorted(price, key = lambda x: x[1])
        
        for i in range(10):
            yield None, price[i]


if __name__ == '__main__':
    Cheapest.run()
