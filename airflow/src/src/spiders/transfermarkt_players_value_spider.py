import scrapy
from scrapy_selenium import SeleniumRequest
from selenium import webdriver
import time
import random
import pandas

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class TheTransfermarkt(scrapy.Spider):
    name = 'transfermarkt_spider'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0',
    }

    def start_requests(self):
        yield SeleniumRequest(
            url='https://www.transfermarkt.com/2022-world-cup/marktwerte/pokalwettbewerb/WM22/pos//detailpos/0/altersklasse/alle',
            wait_time=3,
            screenshot=True,
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response, *args, **kwargs):
        driver = webdriver.Remote(
            command_executor='http://chrome:4444/wd/hub',
            desired_capabilities=DesiredCapabilities.CHROME)
        driver.get('https://www.transfermarkt.com/2022-world-cup/marktwerte/pokalwettbewerb/WM22/pos//detailpos/0/altersklasse/alle')
        time.sleep(random.randint(10, 20))

        from scrapy.shell import inspect_response
        inspect_response(response, self)

        data = []
        pages = range(1, 11)
        columns_len = [1, 2, 3, 4, 5, 6]
        headers = []
        for column_number in columns_len:
            header_content = response.xpath(f'//*[@id="yw1"]/table/thead/tr/th[{column_number}]/text()').get()
            if header_content is None:
                headers.append(response.xpath(f'//*[@id="yw1"]/table/thead/tr/th[{column_number}]/a/text()').get())
            else:
                headers.append(header_content)

        data.append(headers)

        for page in pages:

            rows_len = range(1, 26)

            for row in rows_len:
                each_row = []
                for col in columns_len:
                    if col in (3, 5):
                        element = driver.find_element("xpath", "//*[@id='yw1']/table/tbody/tr[" + str(row) + "]/td[" + str(col) + "]//a[@title]")
                        each_row.append(element.get_attribute("title"))
                    elif col == 2:
                        element = driver.find_element("xpath", "//*[@id='yw1']/table/tbody/tr[" + str(row) + "]/td[" + str(col) + "]/table/tbody/tr[1]/td[2]//a[@title]")
                        each_row.append(element.get_attribute("title"))
                    elif col in (1, 4, 6):
                        element = driver.find_element("xpath", "//*[@id='yw1']/table/tbody/tr[" + str(
                            row) + "]/td[" + str(col) + "]").get_attribute("textContent")
                        each_row.append(element.replace(" ", ""))

                data.append(each_row)

            if page == 1:
                sleep_time = random.randint(15, 25)
                next_page = driver.find_element("xpath", f'//*[@id="yw1"]/div[2]/ul/li[11]/a')
                driver.execute_script("arguments[0].click();", next_page)
                time.sleep(sleep_time)
            elif page != 1 and page < 10:
                sleep_time = random.randint(15, 25)
                next_page = driver.find_element("xpath", f'//*[@id="yw1"]/div[2]/ul/li[13]/a')
                driver.execute_script("arguments[0].click();", next_page)
                time.sleep(sleep_time)
            else:
                print("Data was scraped")

        pd = pandas.DataFrame(data)

        with open('/opt/airflow/data/transfermarkt_players_value.csv', 'w') as file:
            file.write('')

        pd.to_csv("/opt/airflow/data/transfermarkt_players_value.csv", index=False, header=False)
        yield {'row': data}

        driver.quit()


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl('transfermarkt_spider')
    process.start()