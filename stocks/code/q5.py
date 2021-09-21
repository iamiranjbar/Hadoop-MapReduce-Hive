from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol
from mrjob.step import MRStep

class MostClosed(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        name = columns[0]
        try:
            volume = float(columns[3])
            if volume != 0:
                yield name, 1
        except ValueError:
            pass
    
    def reducer_calculate_name_open_days(self, name, activity):
        yield None, (name, sum(activity))
    
    def reducer_sort_by_least_open_days(self, _, values): # Means the most close days
        sorted_values = sorted(list(values), key= lambda x: x[1])
        for i in range(100):
            yield None, sorted_values[i]
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_calculate_name_open_days),
            MRStep(reducer=self.reducer_sort_by_least_open_days)
        ]


if __name__ == '__main__':
    MostClosed.run()
