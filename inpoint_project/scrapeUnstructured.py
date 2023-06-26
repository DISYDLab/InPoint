import csv
from parsel import Selector
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from urllib.request import urlopen
import sys


query = sys.argv[1:][0];
print(query)

driver = webdriver.Chrome("/usr/bin/chromedriver")
sleep(0.5)

driver.get('https://www.google.com/')
search_query = driver.find_element_by_name('q')
#search_query.send_keys('site:wikipedia.org vaccine')
search_query.send_keys(query)
search_query.send_keys(Keys.RETURN)
sleep(0.5)

#urls = driver.find_elements_by_xpath('//*[@class = "g"]/ul')

soup = BeautifulSoup(driver.page_source, 'html.parser')
# soup = BeautifulSoup(r.text, 'html.parser')
urls = []
search = soup.find_all('div', class_="g")
for h in search:
    urls.append(h.a.get('href'))


print(urls)
#urls = [url.get_attribute('href') for url in urls]
sleep(0.5)
cnt = 0;
for url in urls:
    try:
        driver.get(url)
        sleep(2)
        page = urlopen(url)
        if (page.code == 200):
            soup = BeautifulSoup(page, 'html.parser')
            name = soup.text.strip() # strip() is used to remove starting and trailing
            print('---------------------------------------------------')
            print(url)
            print('---------------------------------------------------')
            #print(name)
            #url = driver.current_url
            strurls = url.split('/')
            print(strurls[-1])
            file = open(str(cnt)+query+strurls[-1]+".txt","w");
            file.write(name);
            file.close()
    except Exception as e:
        print(e)
    cnt = cnt + 1;


driver.quit()
