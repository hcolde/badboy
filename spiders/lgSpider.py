# -*- coding: utf-8 -*-
import scrapy
import json
import threading
from .db import Database


class LgspiderSpider(scrapy.Spider):
	name = 'lgSpider'
	allowed_domains = ['m.lagou.com']
	start_urls = [
		r'https://m.lagou.com/search.json?city=%E5%B9%BF%E5%B7%9E&positionName=python&pageNo=1'
	]

	pageSize = 0
	totalCount = 0

	def parse(self, response):
		def create():
			'''
			创建数据表.
			'''
			db = self.connect()
			sql = '''
				CREATE TABLE IF NOT EXISTS `job`(
					`id` INT AUTO_INCREMENT,
					`positionId` INT,
					`positionName` VARCHAR(50),
					`salary` VARCHAR(10),
					`createTime` VARCHAR(10),
					`companyFullName` VARCHAR(50),
					PRIMARY KEY (`id`)
				)ENGINE=InnoDB DEFAULT CHARSET=utf8;
			'''
			sql = sql.replace('\t', '').replace('\n', '')
			db.process(sql=sql)
			db.close()

		t = threading.Thread(target=create)
		t.start()
		t.join()

		self.data_processing(response)

		# 获取分页整数.
		pages = self.totalCount//self.pageSize
		pages = pages+1 if self.totalCount%self.pageSize else pages

		# 构造网址，range包头不包尾.
		for page in range(2, pages+1):
			url = response.url[:-1] + str(page)
			yield scrapy.Request(url, callback=self.data_processing)

	def data_processing(self, response):
		'''
		格式化页面数据.
		查看每页显示多少条信息，以及总共有多少条信息.
		'''

		data = response.body
		# 解析json格式，转化为dict.
		data = data.decode(encoding='utf-8')
		dictData = json.loads(data)['content']['data']

		# 每页显示pageSize条信息.
		self.pageSize = dictData['page']['pageSize']

		# 总共有totalCount条信息.
		self.totalCount = int(dictData['page']['totalCount'])

		self.deal_with(dictData['page']['result'])

	def deal_with(self, results):
		'''
		positionId: 职位id，打开详情页面需要获取的参数.
		name: 职位名称.
		salary: 工资.
		time: 发布时间.
		company: 公司名称.
		'''

		def insert(positionId, name, salary, time, company):
			'''
			插入数据.
			'''

			db = self.connect()
			sql = '''
				INSERT INTO `job`(
					`positionId`,
					`positionName`,
					`salary`,
					`createTime`,
					`companyFullName`
				) VALUES (%d, '%s', '%s', '%s', '%s')
			''' % (positionId, name, salary, time, company)
			sql = sql.replace('\t', '').replace('\n', '')
			ret = db.commit(sql)
			if ret:
				self.log(ret)
			db.close()

		for result in results:
			positionId = result.setdefault('positionId')
			name = result.setdefault('positionName')
			salary = result.setdefault('salary')
			time = result.setdefault('createTime')
			company = result.setdefault('companyFullName')
			t = threading.Thread(target=insert, args=(positionId,
													  name,
													  salary,
													  time,
													  company))
			t.start()
			t.join()

	def connect(self):
		'''
		连接数据库.
		'''

		host='111.230.110.103'
		user='root'
		password='HuanG3213507'
		dbName='lagou'
		db = Database(host, user, password, dbName)
		return db