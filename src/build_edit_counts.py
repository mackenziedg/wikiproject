from mrjob.job import MRJob


class EditCounter(MRJob):

    def mapper(self, _, values):
        page_name = values.split()[3]
        yield (page_name, 1)

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == "__main__":
    EditCounter.run()
