from datetime import datetime
from datetime import timedelta
from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

import csv

from bs4 import BeautifulSoup
from requests import get

SS_URL = 'https://old.reddit.com/r/Superstonk/'
WSB_URL = 'https://old.reddit.com/r/wallstreetbets/'

FILEPATH = './reddit-scraper/data'

def process_soup(soup):
    temp = [datetime.now()]

    user_stats = soup.find_all("span", {"class": "number"})

    temp.append(user_stats[0].contents[0])
    temp.append(user_stats[1].contents[0])

    return temp


@task(retries=3, retry_delay_seconds=10)
def get_superstonk():
    user_agent = 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    headers = {"user-agent": user_agent}

    r = get(SS_URL, headers=headers)

    return BeautifulSoup(r.text, 'lxml')



@task(retries=3, retry_delay_seconds=10)
def get_wsb():
    user_agent = 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    headers = {"user-agent": user_agent}

    r = get(WSB_URL, headers=headers)

    return BeautifulSoup(r.text, 'lxml')


@task
def save_data(data):

    with open(FILEPATH, 'a+', newline='') as file:
        writer = csv.writer(file)

        writer.writerow(data)

        file.close()

    return True


@flow(task_runner=SequentialTaskRunner())
def scrape_reddit_flow():
    """
    Main task runner
    """
    logger = get_run_logger()

    logger.info("Getting Superstonk...")
    try:
        ss_soup = get_superstonk()
    except Exception as e:
        logger.error("Error getting data from Superstonk...")

    logger.info("Getting WallStreetBets...")
    try:
        wsb_soup = get_wsb()
    except Exception as e:
        logger.error("Error getting data from Superstonk...")

    logger.info("Processing the good stuff...")
    ss_data = process_soup(ss_soup)
    wsb_data = process_soup(wsb_soup)

    for item in wsb_data:
        ss_data.append(item)

    logger.info("Saving data...")
    temp = save_data(ss_data)

if __name__ == "__main__":
    scrape_reddit_flow()