import time
import math
import pandas as pd

# Webscraping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

import warnings

# Suppress future warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

TABLE_X_PATH = "//*[@id='tthHoldingsTable']"


def _scrape_table(driver: webdriver, wait: WebDriverWait):
    table = wait.until(
        ec.visibility_of_element_located((By.XPATH, TABLE_X_PATH))
    )  # driver.find_element(By.XPATH, TABLE_X_PATH)
    rows = table.find_elements(By.XPATH, ".//tr")

    table_data = []
    for row in rows:
        # cells = wait.until(
        #     ec.visibility_of_all_elements_located((By.XPATH, ".//th | .//td"))
        # )
        cells = row.find_elements(By.XPATH, ".//th | .//td")
        row_data = [cell.text.strip() for cell in cells]
        table_data.append(row_data)

    return table_data[1:]


def _expand_table(driver, xpath: str = "//a[@perpage='60']"):
    show_items = driver.find_element(By.XPATH, xpath)
    show_items.click()


def _create_driver(options: Options = None, wait_time: int = 5):
    if options is None:
        options = Options()
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(wait_time)
    wait = WebDriverWait(driver, 30, poll_frequency=1)
    return driver, wait


def _next_page(driver: webdriver, page_index: int, pages_per_row: int = 6):
    """
    page_index (int): Index of the current page.
    pages_per_row (int): How many page numbers are visible on the website, default 6, because pages are shown in chunks like this: (1, 2, 3, 4, 5) (6, 7, 8, 9, 10) (11, 12, 13, 14, 15)

    """
    if page_index % pages_per_row == 0:
        # Xpath for the 'Next' button
        xpath = "/html/body/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[3]/div/div/ul[2]/li[7]/a"
    else:
        xpath_index = page_index + 1
        # Xpath for the numerical buttons such as '1', '2', '3', etc.
        xpath = "/html/body/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[3]/div/div/ul[2]/li[{}]/a".format(
            xpath_index
        )
    button = driver.find_element(By.XPATH, xpath)
    button.click()
    return True


def pipeline(etf_symbol: str, wait_time: int = 5, debug: bool = True) -> pl.DataFrame:
    ### Step 1 ###
    # Create a driver object.
    driver, wait = _create_driver(wait_time=wait_time)
    url = (
        "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF"
        "/index.asp?type=holdings&symbol={}".format(etf_symbol)
    )
    driver.get(url)

    data = []
    max_pages_per_row = 6
    total_index = 0
    batch_size = 60
    cur_page = 1
    needs_expansion = True
    while True:
        try:
            if debug:
                batch_step = total_index * batch_size
                print(
                    f"Scraping page {total_index + 1}: {batch_step + 1} - {batch_step + batch_size}"
                )
            # Table only needs to be expanded once, set to false after expanded.
            if needs_expansion:
                _expand_table(driver)
                needs_expansion = False
            time.sleep(1)
            table = _scrape_table(driver=driver, wait=wait)
            data.append(table)
            time.sleep(1)
            # Step to the next page.
            cur_page += 1
            total_index += 1
            _next_page(driver=driver, page_index=cur_page)
            if cur_page % max_pages_per_row == 0:
                cur_page = 1
        except NoSuchElementException:
            break
        except ElementClickInterceptedException as e:
            time.sleep(10)
            if debug:
                print(f"{e}... Waiting for browser to load and retrying...")
            pass
        except StaleElementReferenceException as e:
            time.sleep(10)
            if debug:
                print(f"{e}... Waiting for browser to load and retrying...")
            pass
    # Flatten the nested list
    rows = [row for group in data for row in group]

    df = pd.DataFrame(
        rows, columns=["symbol", "name", "weight", "shares_owned", "shares_value"]
    )
    df = df[df["symbol"]]
    driver.close()
    return df


if __name__ == "__main__":
    df = pipeline("VOO")
