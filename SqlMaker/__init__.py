'''
Created on 29 окт. 2013 г.

@author: garet
'''


class SqlMaker:
    def __init__(self, conn=None, type_db=None, pref='', debug=False, 
                 fetch_type='dict'):
        self.__sql = ''
        self.__params = []
        self.__count_params = 0
        self.__start_where = False
        self.__cursor = None
        self.__debug = debug
        self.__type_db = type_db
        self.__conn = conn
        self.__pref = pref
        self.__fetch_type = fetch_type
        self.__return_id = 'id'
        
    def __str__(self):
        return self.__sql
    
    def __del__(self):
        pass
    
    # Not tested
    def DebugPrint(self):
        str_tmp = 'SQL: \r{0}\r'
        str_tmp += 'Count params: {1}\r'
        str_tmp += 'Params list: {2}'
        print(str_tmp.format(self.__sql, self.__count_params, self.__params))
    
    def Select(self, *args):
        result = ''
        for column in args:
            if type(column) != dict:
                if column == None:
                    raise Exception("SqlMaker: Argument 'Select' can`t be None.")
                result += '{0}, '.format(column)
                continue
            while len(column) > 0:
                item = column.popitem()
                result += '{0} AS {1}, '.format(item[0], item[1])
        result = result.strip(', ')
        if len(args) == 0:
            result = '*'
        self.__sql += 'SELECT {0}\r'.format(result)
        return self

    def From(self, *args):
        result = ''
        if len(args) == 0:
            raise Exception("SqlMaker: Argument 'From' can`t be None.")
        for table in args:
            if type(table) == dict:
                item = table.popitem()
                result += '{0} AS {1}, '.format(item[0], item[1])
            else:
                if table == None:
                    raise Exception("SqlMaker: Argument 'Select' can`t be None.")
                result += '{0}, '.format(table)
        result = result.strip(', ')
        self.__sql += 'FROM {0}\r'.format(result)
        return self

    # Not tested
    def Limit(self, limit, offset = None):
        if type(limit) != int or (type(offset) != int and offset != None):
            raise Exception('SqlBuilder.Limit: Params might only Integer!')
        if offset != None:
            if self.__type_db != 'pg':
                self.__sql += 'LIMIT {0},{1} \r'.format(limit, offset)
            else:
                self.__sql += 'LIMIT {0} OFFSET {1} \r'.format(limit, offset)
        else:
            self.__sql += 'LIMIT {0} \r'.format(limit)
        return self

    def Update(self, table, *args):
        result = ''
        if len(args) == 0:
            raise Exception("SqlMaker: Counts arguments 'Update' is 0.")
        for item in args:
            if type(item) != dict:
                raise Exception("SqlMaker: Argument 'Update' can`t be not dict.")
            values = item.popitem()
            result += '{0} = {1}, '.format(values[0], self.Placeholder())
            self.__params.append(values[1])
            self.__count_params += 1
        result = result.strip(', ')
        sql_tmp = 'UPDATE {0} SET {1}\r'
        self.__sql += sql_tmp.format(table, result)
        return self
    
    # Not tested
    def OrderBy(self, *args):
        str_params = ''
        for param in args:
            str_params += '{0}, '.format(param)
        str_params = str_params.strip(', ')
        self.__sql += 'ORDER BY {0}\r'.format(str_params)
        return self
    
    # Not tested
    def InnerJoin(self, table, condict):
        self.__sql += 'INNER JOIN {0} ON {1}\r'.format(table, condict)
        return self
    
    # Not tested
    def LeftJoin(self, table, condict):
        self.__sql += 'LEFT JOIN {0} ON {1}\r'.format(table, condict)
        return self
    
    # Not tested
    def RightJoin(self, table, condict):
        self.__sql += 'RIGHT JOIN {0} ON {1}\r'.format(table, condict)
        return self
    
    # Not tested
    def FullJoin(self, table, condict):
        self.__sql += 'FULL JOIN {0} ON {1}\r'.format(table, condict)
        return self
    
    def Delete(self, table):
        self.__sql += 'DELETE FROM {0}\r'.format(table)
        return self

    def Where(self, condict, param=None):
        if self.__start_where:
            self.__sql += 'AND {0}\r'.format(condict)
        else:
            self.__sql += 'WHERE \r{0}\r'.format(condict)
            self.__start_where = True
        if param != None:
            self.__count_params += 1
            self.__params.append(param)
        return self

    def WhereOr(self, condict, param=None):
        if self.__start_where:
            self.__sql += 'OR {0}\r'.format(condict)
        else:
            self.__sql += 'WHERE \r{0}\r'.format(condict)
            self.__start_where = True
        if param != None:
            self.__count_params += 1
            self.__params.append(param)
        return self

    # Not tested
    def Command(self, sql, *args):
        # Clear all existed command
        self.Clear()
        # Append sql
        self.__sql = sql
        for param in args:
            self.__params.append(param)
            self.__count_params += 1
        return self

    def Insert(self, table, *args):
        result = ''
        params = ''
        for arg in args:
            values = arg.popitem()
            result += '{0},'.format(values[0])
            params += '{0},'.format(self.Placeholder())
            self.__params.append(values[1])
            self.__count_params += 1
        result = result.strip(', ')
        params = params.strip(', ')
        
        sql_tmp = 'INSERT INTO {0}({1}) VALUES ({2})'
        self.__sql += sql_tmp.format(table, result, params)
        return self

    def Execute(self, *args):
        result = True
        self.__sql = self.__sql.replace('{pref}', self.__pref)
        self.__sql = self.__sql.replace('{ph}', self.Placeholder)
        self.__sql = self.__sql.strip() + ';'
        if self.__debug:
            print(self.__sql)
            print()
        for param in args:
            self.__params.append(param)
            self.__count_params += 1
        if self.__cursor != None:
            self.__cursor.close()
        else:
            self.__cursor = None
        self.__cursor = self.__conn.cursor()
        try:
            if self.__count_params > 0:
                self.__cursor.execute(self.__sql, self.__params)
            else:
                self.__cursor.execute(self.__sql)
        except Exception as e:
            print('Error: {0}'.format(e))
            self.__conn.rollback()
            self.Clear()
            return False
        else:
            result = self.__conn.commit()
            self.Clear()
        return result
    
    def FetchOne(self):
        if self.__fetch_type == 'dict':
            rows = self.__cursor.fetchone()
            if rows is None: return False
            cols = []
            unknow_column_num = 0
            for d in self.__cursor.description:
                if d[0] == '?column?':
                    cols.append('column_{0}'.format(unknow_column_num))
                else:
                    cols.append(d[0])
                unknow_column_num += 1
            return dict(zip(cols, rows))
        else:
            return self.__cursor.fetchone()

    def FetchAll(self):
        if self.__fetch_type == 'dict':
            rows_list = self.__cursor.fetchall()
            result = []
            for rows in rows_list:
                if rows is None: return False
                cols = []
                unknow_column_num = 0
                for d in self.__cursor.description:
                    if d[0] == '?column?':
                        cols.append('column_{0}'.format(unknow_column_num))
                        unknow_column_num += 1
                    else:
                        cols.append(d[0])
                result.append(dict(zip(cols, rows)))
            return result
        else:
            return self.__cursor.fetchall()

    # Not tested
    def InsertId(self):
        if self.__type_db == 'pg':
            return self.FetchOne()[self.__return_id]
        else: 
            return self.__cursor.lastrowid()
    
    # Not tested
    def ReturnId(self, return_id = 'id'):
        self.__return_id = return_id
        self.__sql += ' RETURNING {0}'.format(return_id)
        return self
        
    def Clear(self):
        self.__start_where = False
        self.__count_params = 0
        del(self.__params)
        self.__params = []
        self.__sql = ''
        
    def Placeholder(self):
        return '%s'