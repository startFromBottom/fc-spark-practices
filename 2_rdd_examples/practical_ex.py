from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession


def setup_course_id_chapter_id_rdd(sc: SparkContext, is_test: bool):
    # (course_id, chapter_id)
    if is_test:
        data = [
            (1, 96),
            (1, 97),
            (1, 98),
            (2, 99),
            (3, 100),
            (3, 101),
            (3, 102),
            (3, 103),
            (3, 104),
            (3, 105),
            (3, 106),
            (3, 107),
            (3, 108),
            (3, 109),
        ]
        return sc.parallelize(data)

    return sc.textFile("data/chapters.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), int(row[1])))


def setup_user_id_chapter_id_rdd(sc: SparkContext, is_test: bool):
    # (user_id, chapter_id)
    if is_test:
        data = [
            (14, 96),
            (14, 97),
            (13, 96),
            (13, 96),
            (13, 96),
            (14, 99),
            (13, 100),
        ]

        return sc.parallelize(data)

    return sc.textFile("data/views-*.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), int(row[1])))


def setup_course_id_title_rdd(sc: SparkContext, is_test: bool):
    # (course_id, title)
    if is_test:
        data = [
            (1, "Flutter programming"),
            (2, "Marketing 101"),
            (3, "Apache Spark guide"),
        ]

        return sc.parallelize(data)

    return sc.textFile("data/titles.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), row[1]))


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("rdd examples ver") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    is_test = False

    # 0. 입력 데이터 불러오기.

    # 1) 코스 내의 챕터 데이터 (course_id, chapter_id)
    course_chapter_rdd = \
        setup_course_id_chapter_id_rdd(sc, is_test)

    # 2) 각 유저가 수강한 챕터 데이터 (user_id, chapter_id)
    user_chapter_rdd = \
        setup_user_id_chapter_id_rdd(sc, is_test)

    # 3) 각 코스의 제목 데이터 (course_id, title)
    course_title_rdd = setup_course_id_title_rdd(sc, is_test)

    # 1. 한 코스 내의 챕터 데이터
    # output : (course_id, chapter_count)
    chapter_count_per_course_rdd = \
        course_chapter_rdd.map(lambda row: (row[0], 1)) \
            .reduceByKey(lambda c1, c2: c1 + c2)
    print(f"chapter_count_per_course_rdd => "
          f"{chapter_count_per_course_rdd.collect()}")

    # 2. 중복 제거
    # output : (user_id, chapter_id)
    step2 = user_chapter_rdd.distinct()
    print(f"step2 => {step2.collect()}")

    # 3. chapter_id 를 기준으로 join
    # output : (chapter_id : (user_id, course_id))
    step3 = \
        step2.map(lambda row: (row[1], row[0])) \
            .join(course_chapter_rdd.map(lambda row: (row[1], row[0])))
    print(f"step3 => {step3.collect()}")

    # 4. 유저당 코스 수강 횟수 집계
    # output : ((user_id, course_id), view_count)
    step4 = step3.map(lambda row: (row[1], 1)) \
        .reduceByKey(lambda view_count1, view_count2: view_count1 + view_count2)
    print(f"step4 => {step4.collect()}")

    # 5. user_id 정보 삭제
    # output : (course_id, view_count)
    step5 = step4.map(lambda row: (row[0][1], row[1]))
    print(f"step5 => {step5.collect()}")

    # 6. #5.의 결과와 chapter_count_per_course_rdd 조인
    # output : (course_id : (view_count, chapter_count))
    step6 = step5.join(chapter_count_per_course_rdd)
    print(f"step6 => {step6.collect()}")

    # 7. 각 수강생의, 각 코스별 이수율 계산
    # output : (course_id : view_count / chapter_count)
    step7 = step6.map(lambda row: (row[0], row[1][0] / row[1][1]))
    print(f"step7 => {step7.collect()}")


    # 8. 이수율을 스코어로 변환
    # output : (course_id, score)
    def get_score(row):
        course_id = row[0]
        percentage = row[1]
        score = 0
        if percentage >= 0.9:
            score = 10
        elif percentage >= 0.5:
            score = 4
        elif percentage >= 0.25:
            score = 2
        return course_id, score


    step8 = step7.map(get_score)
    print(f"step8 => {step8.collect()}")

    # 9. 코스 별 최종 스코어 : 각 유저별 스코어의 평균
    # output : (course_id, final_score)

    # reduceByKey 사용 (성능 상 이득)
    # step9 = step8 \
    #     .mapValues(lambda v: (v, 1)) \
    #     .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    #     .mapValues(lambda v: v[0] / v[1]) \

    # groupByKey 사용 (더 직관적인 코드, 성능 면에선 안 좋음)
    step9 = step8.groupByKey().mapValues(lambda x: sum(x) / len(x))
    print(f"step9 => {step9.collect()}")

    # 10. 코스별 제목 데이터(course_title_rdd)와 조인
    # output : (course_title, score)
    step10 = step9.join(course_title_rdd) \
        .map(lambda row: (row[1][1], row[1][0]))
    print(f"step10 => {step10.collect()}")

    # 11. score의 내림 차순으로 출력
    result = step10.collect()
    result.sort(key=lambda v: v[1], reverse=True)
    print("==== final result ====")
    for title, score in result:
        print(f"{title} | score - {score}")
