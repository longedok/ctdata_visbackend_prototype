import psycopg2


class Database(object):
    instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = object.__new__(cls, *args, **kwargs)
            return cls.instance
        else:
            return cls.instance

    def connect(self):
        return psycopg2.connect(database='Example',
                                user='postgres',
                                password='root',
                                host='localhost')


class Dataset(object):
    def __init__(self, table_name):
        self.table_name = table_name
        self.dimensions = []
        self.default_indicator = []
        self._get_dimensions()
        self._get_default_indicator()

    def _get_dimensions(self):
        d = Database()
        conn = d.connect()
        curs = conn.cursor()
        query = '''
            Select column_name from information_schema.columns
            where table_name = %s and column_name <> 'did'
        '''
        curs.execute(query, (self.table_name,))
        for row in curs.fetchall():
            self.dimensions.append(Dimension(row[0]))

    def _get_default_indicator(self):
        pass


class AbsentDataset(Dataset):
    def _get_default_indicator(self):
        return [{'town': ['Andover']}, {'year': ['2008', '2009', '2010']},
                {'grade': ['Grade 3']}, {'measuretype': ['Percent']},
                {'variable': ['Students absent 20 or more days']}]


class DatasetFactory(object):
    def get_dataset(self, name, table_name):
        if name == 'absenteeism':
            return AbsentDataset(table_name)
        if name == 'chronicabsenteeism':
            return AbsentDataset(table_name)


class View(object):
    def __init__(self, dataset):
        self.dataset = dataset

    def get_data(self, filters):
        columns = map(lambda x: x.name, self.dataset.dimensions)
        columns_string = ",".join(columns)
        processed_filters = []
        for f in filters:
            column_name = f['field']
            values = f['values']
            if column_name in ['year', 'town', 'measuretype'] or len(values) > 1:
                result = '%s in (%s)' % (column_name, ','.join(['%s'] * len(values)))
            else:
                result = '%s = %%s' % column_name
            processed_filters.append(result)
        filters_string = " and ".join(processed_filters)
        filters_values = reduce(lambda acc, x: acc + x.values()[0], filters, [])
        query = '''
            SELECT %s FROM %s WHERE %s
        ''' % (columns_string, self.dataset.table_name, filters_string)

        q, v, c = self.get_query(filters)
        d = Database()
        conn = d.connect()
        curr = conn.cursor()
        curr.execute(q, tuple(v))
        result = self._convert_data(map(lambda y: y.strip(), curr.fetchall()), filters, columns)
        conn.commit()

        return result

    def _convert_data(self, data, filters, columns):
        return data


class TableView(View):
    def _determine_mf(self, filters, columns):
        valid_fields = list(set(columns) - set(['year', 'town', 'measuretype']))
        valid_filters = filter(lambda f: f['field'] in valid_fields, filters)
        return (filter(lambda f: len(f['values']) > 1, valid_filters) or valid_filters)[0]['field']

    def _convert_data(self, data, filters, columns):
        mf = self._determine_mf(filters, columns)
        data = {'multifield': mf, 'years': filters['years']}
        for row in data:
            pass



class Dimension(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "Dimension: %s" % self.name


class ViewFactory(object):
    def get_view(self, view_name, dataset):
        if view_name == 'table':
            return TableView(dataset)


class InputConverter(object):
    def convert(self, inpt):
        vf, df = ViewFactory(), DatasetFactory()
        ds = df.get_dataset('chronicabsenteeism', 'chronicabsenteeism')
        view = None
        if inpt['view'] == 'table':
            view = vf.get_view('table', ds)
        return view.get_data(inpt['filters'])


if __name__ == '__main__':
    inpt = {'view': 'table', 'filters': [{'field': 'town', 'values': ['Andover']},
                                         {'field': 'year', 'values': ['2012', '2013']},
                                         {'field': 'grade', 'values': ['K through 12', 'K through 3']}]}

    ic = InputConverter()
    print ic.convert(inpt)