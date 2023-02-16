import scrapy
from scrapy_selenium import SeleniumRequest
from selenium import webdriver
import time
import random
import pandas

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class FbrefPlayersPassingSpider(scrapy.Spider):
    name = 'fbref_players_passing_spider'

    def start_requests(self):
        yield SeleniumRequest(
            url='https://fbref.com/en/comps/1/passing/World-Cup-Stats',
            wait_time=3,
            screenshot=True,
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response, *args, **kwargs):
        driver = webdriver.Remote(
            command_executor='http://chrome:4444/wd/hub',
            desired_capabilities=DesiredCapabilities.CHROME)

        driver.get('https://fbref.com/en/comps/1/passing/World-Cup-Stats')
        time.sleep(random.randint(5, 7))

        data = []

        headers = []
        for th in driver.find_elements("xpath", '//*[@id="stats_passing"]/thead/tr[2]/th'):
            headers.append(th.text)

        data.append(headers)

        rows = driver.find_elements("xpath", '//*[@id="stats_passing"]/tbody/tr')
        rows_len = len(rows) + 1

        columns = driver.find_elements("xpath", '//*[@id="stats_passing"]/thead/tr[2]/th')
        columns_len = len(columns)

        rows_list = [i for i in range(1, rows_len) if i % 26 != 0]

        for row in rows_list:
            each_row = []
            for col in range(1, columns_len):
                if col == 1:
                    element = driver.find_element("xpath", f'//*[@id="stats_passing"]/tbody/tr[' + str(row) + ']/th[1]').text
                    each_row.append(element)
                    element = driver.find_element("xpath", f'//*[@id="stats_passing"]/tbody/tr[' + str(row) + ']/td[' + str(col) + ']').text
                    each_row.append(element)
                else:
                    element = driver.find_element("xpath", f'//*[@id="stats_passing"]/tbody/tr[' + str(row) + ']/td[' + str(col) + ']').text
                    each_row.append(element)
            data.append(each_row)
                # print(data)

        pd = pandas.DataFrame(data)
        with open('/opt/airflow/data/fbref_players_passing.csv', 'w') as file:
            file.write('')
        pd.to_csv("/opt/airflow/data/fbref_players_passing.csv", index=False, header=False)

        yield {'row': data}

        driver.quit()


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl('fbref_players_passing_spider')
    process.start()
