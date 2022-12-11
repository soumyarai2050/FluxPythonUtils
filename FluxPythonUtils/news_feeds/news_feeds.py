import feedparser
from pprint import pprint


class NewsFeeds:
    def __init__(self, service_name: str):
        self.__service_name: str = service_name
        if self.__service_name.lower() == "times of india":
            self.__feeds_url: str = "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"

    def __load_rss_feeds(self):
        rss_feeds = feedparser.parse(self.__feeds_url)
        return rss_feeds

    def xml_parser(self):
        xml_rss = self.__load_rss_feeds()
        return xml_rss.entries

    def stringify_rss(self):
        xml_rss = self.__load_rss_feeds()
        string_rss: str = ""

        for feed in xml_rss.entries:
            for title, content in feed.items():
                string_rss += f"{title}: {content}\n"

        return string_rss


if __name__ == "__main__":
    def run():
        service_name: str = "times of india"
        news_feed: NewsFeeds = NewsFeeds(service_name)
        # pprint(news_feed.xml_parser())
        print(news_feed.stringify_rss())

    run()
