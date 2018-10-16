import psycopg2

DATABASE = "news"


def Init():
    try:
        conn = psycopg2.connect(database=DATABASE)
        global g_Cursor
        g_Cursor = conn.cursor()

    except psycopg2.DatabaseError as e:
        print("Database exception occurred!!!...")
        print(e.pgerror)
        print(e.diag.message_detail)
        g_Cursor = None
        return

    else:
        g_Cursor.execute("""
                        select table_name
                            from information_schema.views
                        where
                            table_name='view_analysiscolumnsconsolidated'""")
        if g_Cursor.rowcount:
            g_Cursor.execute("""drop view view_analysiscolumnsconsolidated""")

        g_Cursor.execute("""
                        create view
                            view_analysiscolumnsconsolidated
                        as
                            select log.path, log.status,
                                    log.time, articles.title, authors.name
                                from log
                        join
                            articles
                                on
                                    log.path like concat('%', articles.slug)
                        join
                            authors
                                on
                                    articles.author = authors.id""")
        conn.commit()


def GetPopularArticles():
    try:
        g_Cursor.execute("""
                        select title, count(*) as hits
                            from view_analysiscolumnsconsolidated
                        where
                            status = '200 OK'
                        group by
                            title
                        order by
                            hits desc
                        limit 3""")
        rows = g_Cursor.fetchall()

        print("\n1. What are the most popular three articles of all time?")
        print("---------------------------------------------------------")
        for row in rows:
            print(""""%s" - %s views""" % (row[0], row[1]))

    except psycopg2.DatabaseError as e:
        print("Database exception occurred!!!...")
        print(e.pgerror)
        print(e.diag.message_detail)


def GetPopularityOfAuthors():
    try:
        # conn = psycopg2.connect(database=DATABASE)

        # cursor = conn.cursor()
        g_Cursor.execute("""
                        select name, count(name) as no_of_views
                            from view_analysiscolumnsconsolidated
                        where
                            status = '200 OK'
                        group by
                            name
                        order by
                            no_of_views desc""")
        rows = g_Cursor.fetchall()

        print("\n2. Who are the most popular article authors of all time?")
        print("---------------------------------------------------------")
        for row in rows:
            print("%s - %s views" % (row[0], row[1]))

    except psycopg2.DatabaseError as e:
        print("Database exception occurred!!!...")
        print(e.pgerror)
        print(e.diag.message_detail)


def GetDaysBasedOnErrorRateRestriction():
    try:
        g_Cursor.execute("""
            WITH
                total_requests as (
                        select time::date as date, count(*)
                            from log
                        group by
                            time::date
                        order
                            by time::date ),
                total_error_requests as (
                        select time::date as date, count(*)
                            from log
                        where
                            status != '200 OK'
                        group by
                            time::date
                        order by
                            time::date ),
                rate_of_error as (
                        select total_requests.date,
                                total_error_requests.count::float /
                                   total_requests.count::float * 100 as perc
                            from total_requests, total_error_requests
                        where
                            total_requests.date = total_error_requests.date )

            select * from rate_of_error where perc > 1;""")
        rows = g_Cursor.fetchall()

        print(
        '\n3. On which days did more than 1% of requests lead to errors?')
        print(
        '--------------------------------------------------------------')
        for row in rows:
            print('{date:%d, %b %Y} - {err_perc:.1f}% errors'.format(
                date=row[0],
                err_perc=row[1]))

    except psycopg2.DatabaseError as e:
        print("Database exception occurred!!!...")
        print(e.pgerror)
        print(e.diag.message_detail)


if __name__ == '__main__':
    Init()
    if g_Cursor is not None:
        GetPopularArticles()
        GetPopularityOfAuthors()
        GetDaysBasedOnErrorRateRestriction()
        g_Cursor.close()
