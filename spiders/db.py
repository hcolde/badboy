import pymysql


class Database:
	db = None
	cursor = None

	def __init__(self, host='127.0.0.1', user='root', password='', dbName=None):
		# 打开数据库连接.
		self.db = pymysql.connect(host=host,
					  user=user,
					  password=password,
					  db=dbName,
					  charset='utf8mb4',
					  cursorclass=pymysql.cursors.DictCursor) 
		# 使用 cursor() 方法创建一个游标对象 cursor.
		self.cursor = self.db.cursor()

	def process(self, sql, ret=False, multi=False):
		'''
		ret: 是否需要获取数据.
		multi: 结果是否为多条数据.
		'''

		# 使用 execute()  方法执行 SQL 查询.
		self.cursor.execute(sql)
		if ret:
			data = None
			if multi:
				# 使用 fetchall() 方法获取单条数据.
				data = self.cursor.fetchall()
			else:
				# 使用 fetchone() 方法获取单条数据.
				data = self.cursor.fetchone()
			return data

	def commit(self, sql):
		try:
			self.cursor.execute(sql)
			# 提交到数据库执行.
			self.db.commit()
			return None
		except Exception as e:
			# 发生错误则回滚
			self.db.rollback()
			return e

	def close(self):
		# 关闭数据库连接.
		self.db.close()

def connect():
	'''
	连接数据库.
	'''

	l = []
	with open('db.txt', 'r') as f:
		for line in f.readlines():
			l.append(''.join(line.split()))
	host=l.pop(0)
	user=l.pop(0)
	password=l.pop(0)
	dbName=l.pop(0)
	db = Database(host, user, password, dbName)
	return db
