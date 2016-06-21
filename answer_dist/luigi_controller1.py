import luigi
from luigi.contrib.spark import SparkSubmitTask

class answer_dist1(SparkSubmitTask):

        app = "final1.py"

        def input(self):
                return luigi.LocalTarget('answer_dist_input.txt')

        def output(self):
                return luigi.LocalTarget('answer_dist_output1.txt')

        def app_options(self):
                return [self.input().path, self.output().path]

