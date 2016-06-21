import luigi
from luigi.contrib.spark import SparkSubmitTask

class answer_dist11(SparkSubmitTask):

        app = "final11.py"

        def input(self):
                return luigi.LocalTarget('answer_dist_output1.txt')

        def output(self):
                return luigi.LocalTarget('answer_dist_output2.txt')

        def app_options(self):
                return [self.input().path, self.output().path]

