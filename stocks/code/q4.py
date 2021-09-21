from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol
from mrjob.step import MRStep

class MostFallInSixMonth(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        name = columns[0]
        try:
            day_change = float(columns[12])
            yield name, day_change
        except ValueError:
            pass
    
    def reducer_calculate_each_name_month_change(self, name, day_change):
        yield None, (sum(day_change), name)
    
    def reducer_sort_for_six_month(self, _, values):
        sorted_values = sorted(list(values))
        for i in range(10):
            change, name = sorted_values[i]
            yield None, (name, change)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_calculate_each_name_month_change),
            MRStep(reducer=self.reducer_sort_for_six_month)
        ]


if __name__ == '__main__':
    MostFallInSixMonth.run()
