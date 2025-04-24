# python -u "d:\BDA\wordFrequency.py" input.txt --word "example"


from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRFrequencyCount(MRJob):

    def configure_args(self):
        super(MRFrequencyCount, self).configure_args()
        self.add_passthru_arg('--word', type=str, help='Word to count the frequency of.', default='example')

    def mapper(self, _, line):
        word = self.options.word.lower()
        for w in WORD_RE.findall(line):
            if w.lower() == word:
                yield (word, 1)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRFrequencyCount.run()
