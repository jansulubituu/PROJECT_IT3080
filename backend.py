import mysql.connector
from mysql.connector import Error
import timeit
"""
require: mysql-connector-python


"""
import mysql.connector
from mysql.connector import pooling

# Cấu hình connection pool
dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "Thanh@0501",
    "database": "qlcc",
    "auth_plugin": 'mysql_native_password'
}

# Tạo connection pool
cnxpool = pooling.MySQLConnectionPool(pool_name="mypool",
                                      pool_size=5,  # Kích thước pool (số lượng kết nối tối đa)
                                      **dbconfig)
def connect_db():
    try:
        cnx = cnxpool.get_connection()
        print("Lấy kết nối từ pool thành công!")
        return cnx
    except Error as e:
        print("Lỗi lấy kết nối từ pool:", e)
        return None

def close_connection(cnx, cursor):
    try:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Trả kết nối về pool thành công!")
    except Error as e:
        print("Lỗi khi trả kết nối về pool:", e)

  
mydb = connect_db()


#if mydb is not None:
 # cursor = mydb.cursor()
#else:
 # print("Database connection failed")
cursor = mydb.cursor()
cursor.execute("show tables")
col_info = {}
for x in cursor.fetchall():
  """#print(x[0])
    command = f"select * from {x[0]} where NULL"
    #print(command)
    cursor.execute(command)
    #print(cursor.description)
    count_col[x[0]] = len(cursor.description)
    cursor.fetchall()"""
  command = f"show columns from {x[0]}"
  cursor.execute(command)
  col_info[x[0]] = [(x[0], x[3]) for x in cursor.fetchall()]
  #print(col_info[x[0]])


def commit(cnx):
  # luu cac thay doi cua database
  cnx.commit()


def create(table_name, value, position=None):
  cnx = connect_db()
  cursor = cnx.cursor()
  try:
    command = "insert into {} {} values ({})".format(
        table_name, f"({','.join(position)})" if position else "",
        ("%s," * len(position if position else col_info[table_name]))[:-1])
    if (len(value) == 0):
      return
    cursor.executemany(command, value)
    commit(cnx)
  finally:
    close_connection(cnx, cursor)


def modify(table_name, position, value, primary_key, index):
  cnx = connect_db()
  cursor = cnx.cursor()
  try:
    list_change = [f"{x[0]} = '{x[1]}'" for x in zip(position, value)]
    command = f"update {table_name} set {','.join(list_change)} where {primary_key} = '{index}'"
    cursor.execute(command)
    commit(cnx)
  finally:
    close_connection(cnx, cursor)


def delete(table_name, conditions):
  cnx = connect_db()
  cursor = cnx.cursor()
  try:
    command = "delete from {} {}".format(
        table_name,
        "where " + ' and '.join([(x[1].replace("$", x[0]))
                                 for x in conditions]) if conditions else "")
    cursor.execute(command)
    commit(cnx)
  finally:
    close_connection(cnx, cursor)


def join_all(table_name):
  if len(table_name) == 0:
    return -1
  flag = [-1] * len(table_name)

  def find(x):
    if flag[x] < 0:
      return x
    flag[x] = find(flag[x])
    return flag[x]

  ans = [f"{x}" for x in table_name]
  for id, x in enumerate(table_name):
    list_name = {v[0] for v in col_info[x]}
    for pos, y in enumerate(table_name):
      if pos == id:
        break
      if find(id) == find(pos):
        continue
      for name in col_info[y]:
        if name[0] in list_name:
          ans[find(
              pos
          )] += f" join ({ans[find(id)]}) on {x}.{name[0]} = {y}.{name[0]} "
          flag[find(id)] = find(pos)
          break
  return ans[find(0)]


#print(join_all(["tai_khoan", "nhan_khau", "ho_gd"]))


def show(table_name,
         column_name=None,
         conditions=None,
         group_by=None,
         special_column_name=None,
         condition_aggressive=None,
         sort_by=None,
         limit=None,
         isLower=False):
    cnx = connect_db()
    cursor = cnx.cursor()
    try:
        prefix = {"*": "*", "": ""}

        def find(x):
            x = x.lower()
            if x in prefix:
                return prefix[x]
            for table in table_name:
                if x in [x[0].lower() for x in col_info[table]]:
                    prefix[x] = f"{table}.{x}"
                    return prefix[x]
            assert (False)
        full_column_name = [find(x) for x in column_name
                            if x] if column_name else []
        command = "select {} from ({}) {} {} {} {} {}"
        all_table = join_all(table_name)
        command = command.format(
            ','.join(full_column_name + ([
                x[1].format(*(find(y) for y in x[0])) + f" as {x[2]}" if x[2] else ""
                for x in special_column_name
            ] if special_column_name else [])),
            all_table,
            "where " +
            ' and '.join([x[1].replace("$", find(x[0]))
                          for x in conditions]) if conditions else "",
            f"group by {find(group_by)}" if group_by else "",
            "having " + ' and '.join([
                x[1].replace(
                    "$", x[0]
                )
                for x in condition_aggressive
            ]) if condition_aggressive else "",
            f"order by {find(sort_by)}" if sort_by else "",
            f"limit {limit}" if limit else "")
        cursor.execute(command)
        return [
            dict(
                zip([
                    x[0].lower() if isLower and isinstance(x[0], str) else x[0]
                    for x in cursor.description
                ], x)) for x in cursor.fetchall()
        ]
    finally:
        close_connection(cnx, cursor)

"""
create("loai_phong", [("1","vip", "100", "100000")])
create("ho_gd", [("1", "onlooker", "1", "777", "1")])
cursor.execute("select * from ho_gd")
for x in cursor:
    print(x)"""
#value = show(["tai_khoan", "ho_gd"], [], [("id_tai_khoan","$ between 1 and 7"), ("id_ho","$ =1")], limit = 2, sort_by="id_tai_khoan")
#show(["tai_khoan"], ["mat_khau"], group_by="mat_khau", condition_aggressive=[("", "count(*) > 1")],limit = 100, sort_by="")
#print(value)
