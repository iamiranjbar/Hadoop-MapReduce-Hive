from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol

class MostExpensive(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        yield None,(columns[0], columns[10])

    def reducer(self, key, price):
        price = sorted(price, key = lambda x: x[1], reverse=True)
        
        for i in range(1, 11):
            yield None, price[i]


if __name__ == '__main__':
    MostExpensive.run()
