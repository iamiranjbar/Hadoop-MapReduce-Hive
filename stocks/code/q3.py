from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol
from mrjob.step import MRStep

class MostRiseInMonth(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        line = line.strip()
        columns = line.split(',')
        name = columns[0]
        date = columns[15]
        try:
            day_change = float(columns[12])
            month = date.split('/')[1]
            yield (month, name), day_change
        except ValueError:
            pass
    
    def reducer_calculate_each_name_month_change(self, key, day_change):
        month, name = key
        yield None, (month, sum(day_change), name)
    
    def reducer_sort_for_each_month(self, _, values):
        sorted_values = sorted(list(values), key = lambda x: (x[0], x[1]), reverse=True)
        month_counter = dict()
        for value in sorted_values:
            month, change, name  = value
            if month not in month_counter or month_counter[month] < 10:
                if month not in month_counter:
                    month_counter[month] = 1
                else:
                    month_counter[month] += 1
                yield None, (name, month, change)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_calculate_each_name_month_change),
            MRStep(reducer=self.reducer_sort_for_each_month)
        ]


if __name__ == '__main__':
    MostRiseInMonth.run()
