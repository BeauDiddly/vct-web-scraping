# Valorant Champions Tour Web Scraper

![banner_picture](banner.jpg)


## Table of Contents

- [About](#about)
- [Using the Data](#using_the_data)
- [Getting Started](#getting_started)
- [Rate Limit](#rate_limit)
- [Usage](#usage)
- [Built With](#built_with)
- [To Do List](#to_do_list)

## About <a name = "about"></a>

An asynchronous web scraper scraping the 2021 - 2023 Valorant Champions Tour data. It scrapes tournaments, players and agents data from vlr.gg.

## Using the Data <a name ="using_the_data"></a>
The data is publicly avaliable on [Kaggle](https://www.kaggle.com/datasets/ryanluong1/valorant-champion-tour-2021-2023-data).

## Getting Started <a name = "getting_started"></a>


### Prerequisites

- Python 3.10 or higher



### Installing


```
$ git clone https://github.com/RyanLuong1/vct-web-scraping.git
$ cd vct-web-scraping
$ pip3 install -r requirements.txt
```

### File Creation

```
$ mkdir cleaned_data all_vct
$ cd cleaned_data
$ mkdir vct_2021 vct_2022 vct_2023
$ cd vct_2021 
$ mkdir agents ids matches players_stats
$ cd ..
$ cd vct_2022
$ mkdir agents ids matches players_stats
$ cd ..
$ cd vct_2023 
$ mkdir agents ids matches players_stats
```


## Rate Limit <a name = "rate_limit"></a>
Please do not increase the scraping rate on the code to prevent overloading the scraper. Although there is [no robots.txt on vlr.gg](https://www.vlr.gg/30777/is-data-scraping-allowed), please do not overload vlr.gg's server.

The scraper is scraping 25 pages at a time for tournaments, players and agents.

## Usage <a name = "usage"></a>



If you want to scrape tournament data
```
$ python3 matches_scraper.py
```

If you want to scrape agents data
```
$ python3 agent_pick_rates_scraper.py
```

If you want to scrape players data
```
$ python3 player_stats_scraper.py
```

## Built With <a name="built_with"></a>
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
- [Pandas](https://pandas.pydata.org/)
- [asyncio](https://docs.python.org/3/library/asyncio.html)
- [AIOHTTP](https://docs.aiohttp.org/en/stable/)
- [Requests](https://requests.readthedocs.io/en/latest/)

## To Do List <a name ="to_do_list"></a>

1. Scrape 2024 data
2. Clean the data as errors arise