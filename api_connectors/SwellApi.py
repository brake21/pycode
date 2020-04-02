import requests
import os
import time
import random
import json
from queue import Queue
from threading import Thread
from threading import Lock
import logging

global_lock = Lock()
list_of_emails = []
customer_details_list = []


class SwellDownloadWorker(Thread):

    def __init__(self, in_queue, out_queue, guid, api_key, name, date):
        super(SwellDownloadWorker, self).__init__()
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.guid = guid
        self.api_key = api_key
        self.name = name
        self.date = date
        self.swellapi = SwellWrapper(self.guid, self.api_key)

    def run(self):
        while True:
            each_page = self.in_queue.get()
            self._get_customer_list(each_page)
            self.in_queue.task_done()

    def _get_customer_list(self, each_page):
        try:
            response = self.swellapi._get_last_updated_customers(self.date, each_page)
            for each_dict in response['customers']:
                self.out_queue.put(each_dict['email'])
                list_of_emails.append(each_dict['email'])
        except Exception as e:
            logging.info(f"Error while getting customer list in thread.. {self.name}, {e}")


class SwellCustomerDetailsWorker(Thread):
    def __init__(self, out_queue, rejectedemail_queue, guid, api_key, name):
        super(SwellCustomerDetailsWorker, self).__init__()
        self.out_queue = out_queue
        self.rejectedemail_queue = rejectedemail_queue
        self.guid = guid
        self.api_key = api_key
        self.name = name
        self.swellapi = SwellWrapper(self.guid, self.api_key)
        self.processed_records = 0

    def run(self):
        while True:
            email = self.out_queue.get()
            self._get_customer_details(email)
            self.out_queue.task_done()

    def _get_customer_details(self, email):
        try:
            response = self.swellapi._get_customer_details_json(email)
        except Exception as e:
            logging.info(f"Error while getting customer details in thread.. {self.name}, {e}")
            self.rejectedemail_queue.put(email)
        else:
            customer_details_list.append(json.dumps(response))
        finally:
            self.processed_records += 1
            # print(f'Swell data download thread {self.name} and processed records : {self.processed_records}')


class SwellRejectedEmailsWorker(Thread):
    def __init__(self, rejectedemail_queue, guid, api_key, name):
        super(SwellRejectedEmailsWorker, self).__init__()
        self.rejectedemail_queue = rejectedemail_queue
        self.guid = guid
        self.api_key = api_key
        self.name = name
        self.swellapi = SwellWrapper(self.guid, self.api_key)
        self.processed_records = 0

    def run(self):
        while True:
            email = self.rejectedemail_queue.get()
            self._get_customer_details(email)
            self.rejectedemail_queue.task_done()

    def _get_customer_details(self, email):
        try:
            response = self.swellapi._get_customer_details_json(email)
        except Exception as e:
            logging.info(f"Error while getting rejected emails in thread.. {self.name}, {e}")
        else:
            customer_details_list.append(json.dumps(response))
        finally:
            self.processed_records += 1


class SwellWrapper(object):
    BASEURL = "https://app.swellrewards.com/api/"
    # endpoints
    ALL_CUSTOMERS = "v2/customers/all/"
    CUSTOMER_DETAILS = "v2/customers/"

    def __init__(self, guid, api_key):
        self.guid = guid
        self.api_key = api_key

    def _makeheaders(self):
        """ Generate headers for the APIget request """

        return {
            'x-guid': f'{self.guid}',
            'x-api-key': f'{self.api_key}'
        }

    def make_api_call(self, url, method='get', api_params=None):
        """ Sends secure request to the Stella API

            Arguments:
                    1.  Method
                    2.  Endpoint
                    3.  Extra parameters
            Kwargs:
            method: HTTP method to send (default GET)
            data: Dictionary of data to send. In case of GET a dictionary
        """
        headers = self._makeheaders()
        max_attempts = 10
        attempts = 0
        while attempts < max_attempts:
            if method == 'get':
                response = requests.get(url, headers=headers, params=api_params)

            else:
                print('Not a get method')
                break
            try:
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logging.info(f"sleeping for {(2 ** attempts) + random.random()}")
                time.sleep((2 ** attempts) + random.random())
                attempts += 1

                if attempts == max_attempts:
                    raise Exception('Error message from Swell: {0}\n'
                                    'Error details: {1}'
                                    .format(e, response.text))
            else:
                break
            finally:
                pass

    def _generate_all_customer_params(self, date, page_number):
        return {"last_seen_at": f'{date}', "page": page_number}

    def _generate_customer_detail_params(self, email):
        return {"customer_email": f'{email}'}

    def _get_last_updated_customers(self, date, page):
        url = os.path.join(self.BASEURL, self.ALL_CUSTOMERS)
        return self.make_api_call(url, 'get', self._generate_all_customer_params(date, page))

    def _get_total_number_of_pages(self, date, records_per_page):
        url = os.path.join(self.BASEURL, self.ALL_CUSTOMERS)
        responses = self.make_api_call(url, 'get', {"last_seen_at": f'{date}', "per_page": f'{records_per_page}'})
        return responses['links']['total_pages']

    def _get_customer_details_json(self, email):
        url = os.path.join(self.BASEURL, self.CUSTOMER_DETAILS)
        return self.make_api_call(url, 'get', self._generate_customer_detail_params(email))

    def download(self, date):
        ts = time.time()
        # get the total number of
        total_pages = self._get_total_number_of_pages(date, 100)
        in_queue = Queue()
        out_queue = Queue()
        rejectedemail_queue = Queue()

        for list_thread in range(5):
            worker1 = SwellDownloadWorker(in_queue, out_queue, self.guid, self.api_key, list_thread, date)
            worker1.setDaemon(True)
            worker1.start()

        for details_thread in range(20):
            worker2 = SwellCustomerDetailsWorker(out_queue, rejectedemail_queue,
                                                 self.guid, self.api_key, details_thread)
            worker2.setDaemon(True)
            worker2.start()

        logging.info(f"total number of pages: {total_pages}")
        for each_page in range(1, total_pages + 1):
            in_queue.put(each_page)

        in_queue.join()
        out_queue.join()
        logging.info(f"time took to process the records: {time.time() - ts}")
        logging.info(f"firstly processed records : {len(customer_details_list)}")

        for rejectedemails_thread in range(5):
            worker2 = SwellRejectedEmailsWorker(rejectedemail_queue,
                                                self.guid, self.api_key, rejectedemails_thread)
            worker2.setDaemon(True)
            worker2.start()

        rejectedemail_queue.join()
        logging.info(f"Total processed records : {len(customer_details_list)}")
        return customer_details_list

