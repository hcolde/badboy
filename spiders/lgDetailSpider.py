# -*- coding: utf-8 -*-
import scrapy
import re
import threading
import random
from .db import Database, connect


class LgdetailSpider(scrapy.Spider):
	name = 'lgDetailSpider'
	allowed_domains = ['m.lagou.com']
	handle_httpstatus_list = [302, 404, 407, 502, 504]

	urls = [] # 所要爬取的job详情url.
	user_agent = [] # user-agent大全.
	cp = []

	def __init__(self):
		'''
		初始化时获取job表里的positionId(爬取详情页面的参数)和id值(建立两表的关系).
		并查询job详情表里已经保存了哪些job的详情信息，对已保存的job不再爬取.
		'''

		# 读取user-agent.
		with open('headers.txt', 'r') as f:
			for line in f.readlines():
				self.user_agent.append(''.join(line.split()))

		db = connect() 
		# 创建job详情表.
		sql = '''
			CREATE TABLE IF NOT EXISTS `job_detail`(
				`positionId` INT,
				`workyear` VARCHAR(15),
				`temptation` VARCHAR(300),
				`info` VARCHAR(300),
				`content` VARCHAR(1000),
				PRIMARY KEY (`positionId`),
				FOREIGN KEY (`positionId`) REFERENCES job(`positionId`)
			)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		sql = sql.replace('\t', '').replace('\n', '')
		db.process(sql=sql)

		# 读取job详情表中已保存的job，并不再对该job详情页面进行爬取.
		id_list = []
		sql = 'SELECT `positionId` FROM `job_detail`'
		exist = db.process(sql=sql, ret=True, multi=True)
		if exist:
			for id_dict in exist:
				id_list.append(id_dict['positionId'])

		# 读取所有job的positionId.
		sql = 'SELECT DISTINCT `positionId` FROM `job`'
		result = db.process(sql=sql, ret=True, multi=True)
		db.close()

		# 构建需要爬取的job详情url.
		for r in result:
			if r['positionId'] not in id_list:
				url = 'https://m.lagou.com/jobs/' + str(r['positionId']) + '.html'
				self.urls.append(url)

	def start_requests(self):
		'''
		若user_agent列表为空，则不进行爬取.
		dont_filter=True: 对已爬取过的url可以进行重复爬取.
		'''

		if self.user_agent:
			c = random.randint(0, len(self.user_agent)-1)
			headers = {
				'User-Agent': self.user_agent[c],
			}
			for url in self.urls:
				yield scrapy.Request(url=url, headers=headers, dont_filter=True)
		else:
			self.log('err:User-Agent list is empty.')

	def parse(self, response):
		'''
		当返回的status非200时，重复爬取该页面.
		'''

		def insert(workyear, temptation, info, content):
			'''
			插入数据.
			workyear: 工作经验.
			temptation: 职位诱惑.
			info: 公司相关信息.
			content: 职位描述.
			'''

			positionId = response.url.split('/')[-1].split('.')[0]
			db = connect()
			sql = '''
				INSERT INTO `job_detail`(
					`positionId`,
					`workyear`,
					`temptation`,
					`info`,
					`content`
				) VALUES (%d, '%s', '%s', '%s', '%s')
			''' % (int(positionId), workyear[:15], temptation[:300], info[:300], content[:1000])
			sql = sql.replace('\t', '').replace('\n', '')
			ret = db.commit(sql)
			if ret:
				self.log(ret)
			db.close()

		if response.status == 200:
			if response.url in self.cp:
				self.log('正在重爬')
			workyear = response.xpath('//div[@class="items"]/span[@class="item workyear"]/span[@class="text"]/text()').extract_first()
			temptation = response.xpath('//div[@class="temptation"]/text()').extract_first()
			info = response.xpath('//div[@class="company activeable"]/div[@class="desc"]/div[@class="dleft"]/p[@class="info"]/text()').extract_first()
			contents = response.xpath('//div[@class="content"]/p/text()').extract()
			try:
				workyear = ''.join(workyear.split())
				temptation = ''.join(temptation.split())
				info = ''.join(info.split())
				content = ''.join(contents)
				content = ''.join(content.split())
			except:
				pass
			else:
				t = threading.Thread(target=insert, args=(workyear, temptation, info, content))
				t.start()
				t.join()
		else:
			self.log('待重爬')
			if response.url not in self.cp:
				self.cp.append(response.url)
			yield scrapy.Request(url=response.url, headers=response.headers, dont_filter=True, callback=self.parse)