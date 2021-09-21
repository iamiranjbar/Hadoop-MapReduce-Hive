#!/usr/bin/env python3
import pyhdfs
import re
from mrjob.job import MRJob
from mrjob.step import MRStep

stopwords = set()


def preprocess_line(line):
    line = line.lower()
    return re.sub('\W+', ' ', line)

def read_stop_words():
    fs = pyhdfs.HdfsClient(hosts='namenode:9870')
    stopwords_file_address = "/data/stopwords.txt"
    with fs.open(stopwords_file_address) as stopwords_file:
        for line in stopwords_file:
            line = line.decode('utf-8')
            stopwords.add(line.rstrip())

class Count(MRJob): 
    def mapper_get_words(self, _, line): 
        cleaned_line = preprocess_line(line)
        for word in cleaned_line.split():
            yield(word, 1) 

    def reducer_stop_words_removal(self, word, counts): 
        if word not in stopwords:
            # yield(word, sum(counts)) 
            yield(None, (sum(counts), word))

    def reducer_sort_by_count(self, _, values):
        sorted_values = sorted(list(values), reverse=True)
        return sorted_values[:10]
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words, reducer=self.reducer_stop_words_removal),
            MRStep(reducer=self.reducer_sort_by_count)
        ]

if __name__ == '__main__': 
    read_stop_words()
    Count.run()