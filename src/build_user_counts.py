from collections import Counter
from mrjob.job import MRJob

valid_pages = []
with open("../data/edit_counts.tsv", "r") as f:
    for line in f:
        valid_pages.append(line.strip().split("\t"))
        valid_pages[-1][1] = int(valid_pages[-1][1])
print(sorted(valid_pages, key=lambda x: x[1], reverse=True))
valid_pages = [page[0] for page in valid_pages[:10000]]

class BuildUserVectors(MRJob):

    def mapper(self, _, values):
        line = values.split(chr(30))[0].split()
        uid = line[6]
        page = line[3]
        if page in valid_pages:
            yield (uid, page)

    def reducer(self, key, values):
        ucounts = Counter()
        for page in values:
            ucounts[page] += 1
        yield (key, ucounts)

if __name__ == '__main__':
    BuildUserVectors.run()

