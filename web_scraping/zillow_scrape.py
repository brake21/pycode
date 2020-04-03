# Version: 0.0
# Author : Rakesh
import requests
from bs4 import BeautifulSoup as bs


URL = 'xxxx'

headers = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'}

page = requests.get(URL, headers=headers)

soup = bs(page.content, 'html.parser')

house_list = soup.findAll("div", {"class": "list-card-info"})

for each_house in house_list:
    # each_house_list = each_house.findAll("a", {"class": "list-card-link"})
    # # for wrapper in each_house_list[0].findAll("address", {"class": "list-card-addr"}):
    url = each_house.find('a', href=True)
    print(url['href'])
    for wrapper in each_house:
        for sub in wrapper.contents:
            if ( 'bds' in sub.text ):
                print(sub.text.replace('bds', 'bds & ').replace('ba', 'ba & '))
            else:
                print(sub.text)
    print("=======================")
    break