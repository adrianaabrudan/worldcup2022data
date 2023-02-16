import scrapy
from scrapy_selenium import SeleniumRequest
from selenium import webdriver
import time
import random
import pandas

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class FifaWorldRankingSpider(scrapy.Spider):
    name = 'fifa_world_ranking_spider'

    def start_requests(self):
        yield SeleniumRequest(
            url='https://www.fifa.com/fifa-world-ranking/men?dateId=id13869',
            wait_time=3,
            screenshot=True,
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response, *args, **kwargs):
        driver = webdriver.Remote(
            command_executor='http://chrome:4444/wd/hub',
            desired_capabilities=DesiredCapabilities.CHROME)

        driver.get('https://www.fifa.com/fifa-world-ranking/men?dateId=id13869')
        time.sleep(random.randint(5, 7))

        pages = range(1, 6)
        columns = [1, 3, 4, 5, 6]
        data = []

        headers = []
        for index, th in enumerate(driver.find_elements("xpath", '//*[@id="content"]/main/section[2]/div/div/div[1]/table/thead/tr/th')):
            if index != 6:
                headers.append(th.text)

        data.append(headers)

        for page in pages:
            rows = driver.find_elements("xpath", '//*[@id="content"]/main/section[2]/div/div/div[1]/table/tbody/tr')
            rows_len = len(rows) + 1

            for row in range(1, rows_len):
                each_row = []
                for col in columns:
                    element = driver.find_element("xpath", f'//*[@id="content"]/main/section[2]/div/div/div[1]/table/tbody/tr[' + str(row) + ']/td[' + str(col) + ']').text
                    each_row.append(element)

                data.append(each_row)

            if page < 5:
                sleep_time = random.randint(15, 25)
                next_page = driver.find_element("xpath", '//*[@id="content"]/main/section[2]/div/div/div[2]/div/div/div/div/div[3]/div/button')
                driver.execute_script("arguments[0].click();", next_page)
                time.sleep(sleep_time)
            else:
                print("Data was scraped")

        pd = pandas.DataFrame(data)

        with open('/opt/airflow/data/fifa_world_ranking.csv', 'w') as file:
            file.write('')

        pd.to_csv("/opt/airflow/data/fifa_world_ranking.csv", index=False, header=False)
        yield {'row': data}

        driver.quit()


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl('fifa_world_ranking_spider')
    process.start()
