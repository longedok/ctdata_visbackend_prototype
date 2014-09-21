import sys
import csv
import psycopg2


if __name__ == '__main__':
    conn = psycopg2.connect(database='Example', user='postgres', password='root', host='localhost')
    csv_filename = sys.argv[1]
    table_name = sys.argv[2]
    with open(csv_filename, 'rb') as f:
        columns = map(lambda x: x[1:-1], f.readline().strip().split(","))
        columns_string = ",".join(["%s char(80)" % column for column in columns])
        reader = csv.reader(f)
        query = '''
            CREATE TABLE IF NOT EXISTS %s (
              did SERIAL PRIMARY KEY,
              %s
            )
        ''' % (table_name, columns_string)
        curr = conn.cursor()
        curr.execute(query)
        values_string = ",".join(['%s']*len(columns))
        # batch_values = ",".join(['(' + values_string + ')'] * 10) + ';'
        values = "(DEFAULT, %s)" % ",".join(['%s'] * len(columns))
        insert_query = '''
            INSERT INTO %s VALUES %s
        ''' % (table_name, values)
        print insert_query
        for i, row in enumerate(reader):
            curr.execute(insert_query, tuple(row))
            print '.'
        print reader.line_num
        conn.commit()