import os
import sys
import time
import requests
import random
from queue import Queue
from threading import Thread

output_list = []
__BASE_URL = "https://swapi.co/api/"
# endpoints
__PEOPLE_ENDPOINT = "people/"

class SwapWrapper(object):

    def __init__(self):
        pass

    def make_api_call(self, url, method='get'):
        max_attempts = 10
        attempts = 0
        while attempts < max_attempts:
            if method == 'get':
                response = requests.get(url)

            else:
                print('Not a get method')
                break
            try:
                response.raise_for_status()
                return response.json()
            except Exception as e:
                time.sleep((2 ** attempts) + random.random())
                attempts += 1

                if attempts == max_attempts:
                    raise Exception(f'Error While making api call: {e}')


class DownloadDetailsWorker(Thread):

    def __init__(self, in_queue):
        super(DownloadDetailsWorker, self).__init__()
        self.in_queue = in_queue
        self.swapapi = SwapWrapper()

    def run(self):
        while True:
            person = self.in_queue.get()
            person_name = person['name']
            planet_url = person['homeworld']
            ship_url = person['starships']
            planet_name = self._get_details(planet_url)
            ship_name = self._get_details(ship_url)
            output_dict = {f"{person_name}" : { "planet_name" : planet_name, "ship_name": ship_name} }
            output_list.append(output_dict)
            self.in_queue.task_done()

    def _get_details(self, url):
        if url:
            response = self.swapapi.make_api_call(url)
            return response['name']
        else:
            return None

def validate_url(url):
    if (len(url) == 0):
        return None
    else:
        return url

def download():
    in_queue = Queue()
    num_threads = 5
    for i in range(num_threads):
        worker = DownloadDetailsWorker(in_queue)
        worker.setDaemon(True)
        worker.start()

    wrapper = SwapWrapper()
    nextpage = os.path.join(__BASE_URL, __PEOPLE_ENDPOINT)
    while nextpage:
        print(nextpage)
        response = wrapper.make_api_call(nextpage)
        people_list = response['results']
        for each_person in people_list:
            name = each_person["name"]
            planet_url = validate_url(each_person["homeworld"])
            ship_url = validate_url(each_person["starships"])
            if ship_url:
                for each_starship_url in ship_url:
                    pass_dict = {"name" : name, "homeworld": planet_url, "starships": each_starship_url}
                    in_queue.put(pass_dict)
            else:
                pass_dict = {"name": name, "homeworld": planet_url, "starships": None}
                in_queue.put(pass_dict)
        nextpage = response['next']
    in_queue.join()

if __name__ == '__main__':
    try:
        download()
        print(output_list)
    except Exception as e:
        raise Exception(f"Error processing the records :{e}")
    print('Script completed successfully')
    sys.exit(0)
