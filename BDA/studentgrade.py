from mrjob.job import MRJob

class MRStudentGrades(MRJob):

    def mapper(self, _, line):
        # Assuming the input format is: student_id,score
        student_id, score = line.split(',')
        score = int(score)
        yield student_id, score

    def reducer(self, key, values):
        # Assuming a student might have multiple scores, we take the average
        scores = list(values)
        avg_score = sum(scores) / len(scores)
        
        # Assign grade based on average score
        if avg_score >= 90:
            grade = 'A'
        elif avg_score >= 80:
            grade = 'B'
        elif avg_score >= 70:
            grade = 'C'
        elif avg_score >= 60:
            grade = 'D'
        else:
            grade = 'F'

        yield key, grade

if __name__ == '__main__':
    MRStudentGrades.run()
    