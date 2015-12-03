#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging, json, re, datetime
from grab.spider import Spider, Task
from grab import Grab

results_file = open('results_file.txt', 'w+')

class My_Spider(Spider):
	results = []

	def check_page(self, url):
		g = Grab()
		print(url)
		g.go(url)
		try:
			g.doc.select('//div[@id="no_stories_msg"]').text()
		except:
			return True
		else:
			return False

	def get_date(self, timestamp):
		return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

	def task_generator(self):
		count = 1
		url = ''

		while True:
			url = 'http://pikabu.ru/hot?page=%s' % count

			if self.check_page(url) == False:
				break

			yield Task('all_post', url)
			count += 1

			if count > 1: break

	def task_all_post(self, grab, task):
		for post in grab.doc.select('//table[contains(@class, "b-story")]//a[contains(@class, "b-story__link")]'):
			yield Task('post', post.attr('href'))

	def task_post(self, grab, task):
		rating = {
			'plus': grab.doc.select('//div[contains(@class, "b-story__rating")]')[0].attr('data-pluses'),
    		'minus': grab.doc.select('//div[contains(@class, "b-story__rating")]')[0].attr('data-minuses'),
		}
		number_comments = int(re.findall('(\d+)', grab.doc.select('//div[@class="b-story-info__main"]').text())[0])
		title = grab.doc.select('//h1').text()
		
		content = []
		content.append({
			'type': 'text',
			'value': grab.doc.select('//div[contains(@class, "b-story__content")]').text(),
		})
		for img in grab.doc.select('//div[contains(@class, "b-story__content")]//img'):
			content.append({
				'type': 'img',
				'value': img.attr('src'),
			})
		for gif in grab.doc.select('//div[contains(@class, "b-story__content")]//a[contains(@class, "b-gifx__state")]'):
			content.append({
				'type': 'gif',
				'value': gif.attr('href'),
			})
		for video in grab.doc.select('//div[contains(@class, "b-story__content")]//div[@class="b-video"]'):
			content.append({
				'type': 'video',
				'value': video.attr('data-url'),
			})

		tags = []
		for tag in grab.doc.select('//tr[contains(@class, "b-story__header")]//span[contains(@class, "tag")]'):
			tags.append(tag.text())

		author = grab.doc.select('//div[contains(@class, "b-story__header-additional")]//a')[1].attr('href')
		date = datetime.datetime.fromtimestamp(int(grab.doc.select('//div[contains(@class, "b-story__header-additional")]//a[contains(@class, "detailDate")]').attr('title'))).strftime('%Y-%m-%d %H:%M:%S')

		comments = []
		for x in range(0, number_comments):
			comments.append({
				'id': grab.doc.select('//div[@class="b-comment"]')[x].attr('data-id'),
				'parent_id': grab.doc.select('//div[@class="b-comment"]')[x].attr('data-parent-id'),
				'author': grab.doc.select('//div[@class="b-comment"]//div[@class="b-comment__user"]//a')[x].text(),
				'date': self.get_date(int(grab.doc.select('//div[@class="b-comment"]//div[@class="b-comment__user"]//time')[x].attr('datetime'))),
				'text': grab.doc.select('//div[@class="b-comment"]//div[@class="b-comment__content"]')[x].text(),
				'rating': grab.doc.select('//div[@class="b-comment"]//div[@class="b-comment__rating-count"]')[x].text(),
			})


		obj = {
			'url': task.url,
			'rating': rating,
			'number_comments': number_comments,
			'title': title,
			'content': content,
			'tags': tags,
			'author': author,
			'date': date, 
			'comments': comments,
		}
		self.results.append(json.dumps(obj))


if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG, filename = 'logging.txt')
    g = My_Spider(thread_number = 4)
    g.run()
    # g.results_file
