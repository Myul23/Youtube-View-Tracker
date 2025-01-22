from os.path import exists
from time import sleep, localtime, strftime

from numpy.random import randint
from pandas import DataFrame, read_csv, concat

from selenium.webdriver import Edge
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service


class View_Scrapper:
    def __init__(self):
        self.min_second = 3
        self.max_second = 10
        self.__setup()

    def __setup(self) -> None:
        self.browser = Edge(service=Service(executable_path="./msedgedriver.exe"))
        self.browser.maximize_window()

    def __scroll(self, url: str) -> None:
        self.browser.get(url)
        sleep(randint(self.min_second, self.max_second))

        # ? scroll browser down (loading)
        prev_height = self.browser.execute_script("return document.documentElement.scrollHeight")
        while True:
            self.browser.execute_script("window.scrollBy(0, document.documentElement.scrollHeight);")
            sleep(randint(self.min_second, self.max_second))
            curr_height = self.browser.execute_script("return document.documentElement.scrollHeight")

            if curr_height == prev_height:
                try:
                    self.browser.find_element_by_name("more-res").click()
                    sleep(randint(self.min_second, self.max_second))
                except:
                    break
            prev_height = curr_height

    def __scrap_video_infoes(self, links: list) -> list:
        views = []
        for link in links:
            try:
                self.browser.get(link)
                sleep(randint(self.min_second, self.max_second))
                self.browser.find_element(By.XPATH, '//*[@id="description-inner"]').find_element(By.ID, "expand").click()
                sleep(randint(self.min_second, self.max_second))

                # * infoes
                title = self.browser.find_element(By.XPATH, '//*[@id="title"]/h1/yt-formatted-string').text.strip('"')
                video_id = link.split("https://www.youtube.com/watch?v=", 1)[1]
                view = self.browser.find_element(By.XPATH, '//*[@id="info"]/span[1]').text
                view = int(view.split(" ", 1)[1][:-1].replace(",", ""))
                views.append((title, video_id, view))
            except:
                continue
        return views

    def __scrap_short_infoes(self, links: list) -> list:
        views = []
        for link in links:
            try:
                self.browser.get(link)
                sleep(randint(self.min_second, self.max_second))
                self.browser.find_element(By.XPATH, '//*[@id="button-shape"]').find_element(By.TAG_NAME, "button").click()
                sleep(randint(self.min_second, self.max_second))
                self.browser.find_element(By.XPATH, '//*[@id="items"]/ytd-menu-service-item-renderer[1]').click()
                sleep(randint(self.min_second, self.max_second))

                # * infoes
                title = self.browser.find_element(By.XPATH, '//*[@id="title"]/yt-formatted-string/span[1]').text.strip('"')
                video_id = link.split("https://www.youtube.com/watch?v=", 1)[1]
                view = self.browser.find_element(
                    By.XPATH, '//*[@id="factoids"]/view-count-factoid-renderer/factoid-renderer/div/span[1]/span'
                ).text
                view = int(view.replace(",", ""))
                views.append((title, video_id, view))
            except:
                continue
        return views

    def __count_channel_view(self, url: str) -> list:
        self.__scroll(url)
        videos = self.browser.find_elements(By.XPATH, '//*[@id="contents"]/ytd-rich-item-renderer')
        links = [video.find_element(By.TAG_NAME, "ytd-thumbnail").find_element(By.TAG_NAME, "a").get_attribute("href") for video in videos]
        return self.__scrap_video_infoes(links)

    def __count_shorts_view(self, url: str) -> list:
        self.__scroll(url)
        shorts = self.browser.find_elements(By.XPATH, '//*[@id="contents"]/ytd-rich-item-renderer')
        links = [
            short.find_element(By.TAG_NAME, "ytm-shorts-lockup-view-model").find_element(By.TAG_NAME, "a").get_attribute("href")
            for short in shorts
        ]
        return self.__scrap_short_infoes(links)

    def add_data(
        self, links: list, allow_platform: list = ["youtube"], save_flag: bool = True, file_name: str = "video_view_data.csv"
    ) -> None | DataFrame:
        if self.browser is None:
            print("Please Set Up")
            return

        date = strftime("%Y/%m/%d", localtime())
        new_views = DataFrame([], columns=["title", "video_id", date])

        for link in links:
            if sum([platform in link for platform in allow_platform]) < 1:
                print(f"The URL({link}) doesn't contain any accepted platform({allow_platform})")
                continue

            views = self.__count_shorts_view(link) if link.endswith("shorts") else self.__count_channel_view(link)
            views = DataFrame(views, columns=["title", "video_id", date])
            new_views = concat([new_views, views])

        if not save_flag:
            return new_views

        if not exists(file_name):
            new_views.to_csv(file_name, index=False)
        else:
            before = read_csv(file_name)
            data = concat([before, new_views])
            data.to_csv(file_name, index=False)

    def __del__(self):
        self.browser.quit()


if __name__ == "__main__":
    scrapper = View_Scrapper()
    links = [
        "https://www.youtube.com/@CGRN7A/videos",
        "https://www.youtube.com/@Cheong_Run/videos",
        "https://www.youtube.com/@Cheong_Run/streams",
        "https://www.youtube.com/@Cheong_Run/shorts",
    ]
    scrapper.add_data(links=links)
