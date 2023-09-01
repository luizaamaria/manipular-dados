import mysql.connector
from main import *
import pandas as pd


def query_teacher(connection):
    q1 = """
    SELECT *
    FROM teacher;
    """
    results = read_query(connection, q1)

    for result in results:
        print(result)


if __name__ == "__main__":
    connection = create_db_connection(host_name, user_name, user_password, db_name)
    query_teacher(connection)
    connection.close()


def query_course(connection):
    q2 = """
    SELECT *
    FROM course;
    """
    results = read_query(connection, q2)

    for result in results:
        print(result)


def delete_course(connection):
    delete_query = """
    DELETE FROM course
    WHERE course_id = 19;
    """
    execute_query(connection, delete_course)


if __name__ == "__main__":
    connection = create_db_connection(host_name, user_name, user_password, db_name)
    delete_course(connection)
    connection.close()


def query_courses_not_in_school(host_name, user_name, user_password, db_name):
    q5 = """
    SELECT course.course_id, course.course_name, course.language, client.client_name, client.address
    FROM course
    JOIN client
    ON course.client = client.client_id
    WHERE course.in_school = FALSE;
    """

    connection = create_db_connection(host_name, user_name, user_password, db_name)
    results = read_query(connection, q5)
    from_db = []

    for result in results:
        result = list(result)
        from_db.append(result)

    columns = ["course_id", "course_name", "language", "client_name", "address"]
    df = pd.DataFrame(from_db, columns=columns)
    connection.close()
    return df


if __name__ == "__main__":
    df = query_courses_not_in_school(host_name, user_name, user_password, db_name)

    print(df)


def insert_teacher(host_name, user_name, user_password, db_name):
    val = [
        (10, 'Luiza', 'Dodson', 'ENG', None, '1990-12-23', 11001, '+491772345678'),
        (11, 'Maria', 'Perkins', 'MAN', 'ENG', '1980-02-02', 23232, '+491443456432')
    ]
    sql = '''
            INSERT INTO teacher (teacher_id, first_name, last_name, language_1, language_2, dob, tax_id, phone_no)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
    connection = create_db_connection(host_name, user_name, user_password, db_name)
    execute_list_query(connection, sql, val)

    q1 = """
        SELECT *
        FROM teacher;
        """

    results = read_query(connection, q1)

    columns = ["teacher_id", "first_name", "last_name", "language_1", "language_2", "dob", "tax_id", "phone_no"]

    df = pd.DataFrame(results, columns=columns)
    connection.close()
    return df


if __name__ == "__main__":
    df = insert_teacher(host_name, user_name, user_password, db_name)
    print(df)


def update_client_address(host_name, user_name, user_password, db_name, new_address, client_name):
    update_query = f"""
    UPDATE client
    SET address = '{new_address}'
    WHERE client_name = '{client_name}';
    """

    connection = create_db_connection(host_name, user_name, user_password, db_name)
    execute_query(connection, update_query)
    connection.close()


if __name__ == "__main__":
    new_address = '23000 Fingiertweg, 14534 Berlin'
    client_name = 'Big Business Federation'

    update_client_address(host_name, user_name, user_password, db_name, new_address, client_name)
