# -*- coding: utf-8 -*-
import scrapy
import re


class hcxySpider(scrapy.Spider):
    name = 'hcxySpider'
    allowed_domains = ['cjcx.hcnu.edu.cn']
    start_urls = ['http://cjcx.hcnu.edu.cn/get-cjcx/get-cj']

    def start_requests(self):
    	yield scrapy.FormRequest(
    			url=self.start_urls[0],
    			formdata={
    				'xh': '2014107115',
    				'mm': '664205233sama.',
    				'xn': 'all-xn',
    				'xq': '1'})

    def parse(self, response):
    	#body = response.body.decode('utf-8')
    	content = response.xpath('//tbody/tr/td').extract()
    	count = 1
    	grade = 0
    	for data in content:
    		if not count%4:
    			pattern = re.compile(r'<td>(.*)</td>')
    			data_ = pattern.search(data)
    			data__ = data_.group(1)
    			if data__ == '优秀':
    				grade += 95
    			elif data__ == '良好':
    				grade += 85
    			elif data__ == '中等':
    				grade += 75
    			elif data__ == '及格':
    				grade += 65
    			elif data__ == '不及格':
    				grade += 50
    			elif len(data__) > 3:
    				grade += 60
    			else:
    				try:
    					grade += int(data__)
    				except:
    					grade += 0
    		count += 1
    	self.log('总分:' + str(grade))
    	self.log('平均分:' + str(grade/((count-1)/4)))