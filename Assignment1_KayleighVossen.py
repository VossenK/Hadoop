# Import necessary classes from MRJob
from mrjob.job import MRJob
from mrjob.step import MRStep


# Set up class
class CountRatings (MRJob):

        # Variable that stores sequential MRJob steps.
        def steps(self):
                return [
                        MRStep(
                        mapper=self.mapper_get_relevant_info,
                        reducer = self.reducer_ratingcount
                        )
                ]

        # Get movieID and rating from imported file, expose them to be used by reducer.
        def mapper_get_relevant_info(self, _, line):
                (userID, movieID, rating, timestamp)  = line.split('\t')
                yield movieID, 1
                yield rating, 1

        # Fill shorter IDs to 3 digits.
        # Get the sum of rows for each movieID.
        def reducer_ratingcount (self, key, values):
                yield key.zfill(3), sum(values)

# Run CountRatings whenever $ python <this_file.py> <file> is ran.
if __name__ == '__main__':
        CountRatings.run()
