# -*- coding: utf-8 -*-
import scrapy
import json
import threading
from .db import Database, connect


class LgspiderSpider(scrapy.Spider):
	name = 'lgSpider'
	allowed_domains = ['m.lagou.com']
	start_urls = [
		r'https://m.lagou.com/search.json?city=%E5%B9%BF%E5%B7%9E&positionName=python&pageNo=1'
	]

	pageSize = 0 # 每页显示多少条信息.
	totalCount = 0 # 总共有多少条信息.
	id_list = [] # 数据库已保存的job's positionId.

	def __init__(self):
		'''
		初始化时查询数据库里已经保存了哪些job(positionId)，
		对已保存的job不再爬取.
		'''
		
		def createAndSelect():
			db = connect()
			sql = '''
				CREATE TABLE IF NOT EXISTS `job`(
					`positionId` INT,
					`positionName` VARCHAR(50),
					`salary` VARCHAR(10),
					`createTime` VARCHAR(10),
					`companyFullName` VARCHAR(50),
					PRIMARY KEY (`positionId`)
				)ENGINE=InnoDB DEFAULT CHARSET=utf8;
			'''
			sql = sql.replace('\t', '').replace('\n', '')
			db.process(sql=sql)
			sql = 'SELECT DISTINCT `positionId` FROM `job`'
			result = db.process(sql=sql, ret=True, multi=True)
			for id_dict in result:
				self.id_list.append(id_dict['positionId'])
			db.close()

		t = threading.Thread(target=createAndSelect)
		t.start()
		t.join()

	def parse(self, response):
		

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

			db = connect()
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
			else:
				self.log('append a new job:'+str(positionId))
			db.close()

		for result in results:
			positionId = result.setdefault('positionId')
			name = result.setdefault('positionName')
			salary = result.setdefault('salary')
			time = result.setdefault('createTime')
			company = result.setdefault('companyFullName')
			if positionId not in self. id_list:
				self.log('new job:'+str(positionId))
				t = threading.Thread(target=insert,args=(positionId,
														 name,
														 salary,
														 time,
														 company))
				t.start()
				t.join()