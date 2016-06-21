import luigi
from luigi.contrib.spark import SparkSubmitTask

class course_enroll(SparkSubmitTask):
	
	app = "final.py"

	def input(self):
                return luigi.LocalTarget('abcd/logs/data1.txt')

        def output(self):
                return luigi.LocalTarget('CourseEnrollmentEventsPerDayMixin_OUTPUT8temp.txt')

        def app_options(self):
                return [self.input().path, self.output().path]

