import requests
from lxml import etree
import time
import random
import json
import re
from mysql_helper import MySQLHelper

#  数据库配置与双表初始化
def init_db():
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)['mysql']

    target_db = config['database']
    temp_helper = MySQLHelper(host=config['host'], user=config['user'], password=config['password'], database="",
                              port=config['port'])
    temp_helper.execute_non_query(f"CREATE DATABASE IF NOT EXISTS {target_db}")

    helper = MySQLHelper(host=config['host'], user=config['user'], password=config['password'], database=target_db,
                         port=config['port'])
    #  创建电影信息表（movies_info）
    helper.execute_non_query("""
        CREATE TABLE IF NOT EXISTS movies_info (
            movie_rank INT PRIMARY KEY, title VARCHAR(100) NOT NULL, director VARCHAR(100),
            actors TEXT, publish_year INT, country VARCHAR(50), genre VARCHAR(100),
            rating FLOAT, reviews_count INT
        )
    """)
    #  创建评论表（movie_comments），外键关联 movies_info
    helper.execute_non_query("""
        CREATE TABLE IF NOT EXISTS movie_comments (
            id INT PRIMARY KEY AUTO_INCREMENT, movie_rank INT, commenter VARCHAR(50),
            content TEXT,
            FOREIGN KEY (movie_rank) REFERENCES movies_info(movie_rank) ON DELETE CASCADE
        )
    """)
    helper.execute_non_query("TRUNCATE TABLE movie_comments")
    helper.execute_non_query("DELETE FROM movies_info")
    print(" 数据库双表准备就绪！")
    return helper


# ==========================================
# 请求头（目前用的是自己的有效 Cookie，后续可改，纯随机cookie无法访问评论区内容，但可以访问影名，导演，国家，评分等数据）
# ==========================================
def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Cookie': 'bid=yryd5ZARoz4; _pk_id.100001.4cf6=314a9749559438cc.1776318014.; __utmc=30149280; __utmc=223695111; __yadk_uid=nWquXLdWq8Zag6mPXnuEbNkshvP8uir3; ll="108090"; _vwo_uuid_v2=DF7AF59A819D6A2432D9DBCFC5BE29984|a6369123aef259238c5fd658e372071d; __utmz=30149280.1776319945.2.2.utmcsr=sec.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utma=30149280.1823814052.1776318014.1776319945.1776435769.3; dbcl2="294694644:vw0Sq6Kzjb4"; ck=cyvI; frodotk_db="bd394f3c08d4d93237abedb41a7e8b96"; ap_v=0,6.0; push_noty_num=0; push_doumail_num=0; __utmv=30149280.29469; __utmt=1; __utmb=30149280.5.10.1776435769; __utma=223695111.1442080203.1776318014.1776319946.1776436730.3; __utmz=223695111.1776436730.3.3.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1776436730%2C%22https%3A%2F%2Fwww.douban.com%2F%22%5D; _pk_ses.100001.4cf6=1; __utmb=223695111.3.10.1776436730'
    }

#  安全提取函数，没爬到内容则返回空字符串而不报错
def safe_extract(xpath_list, default=""):
    return xpath_list[0].strip() if xpath_list else default


#  爬虫
def scrape_movie_details_and_comments(helper):
    print("开始执行双层爬取")
    total_movies = 0

    for page in range(4):
        start = page * 25
        list_url = f'https://movie.douban.com/top250?start={start}'

        try:
            response = requests.get(list_url, headers=get_headers(), timeout=10)
            html = etree.HTML(response.text)
            items = html.xpath('//div[@class="item"]')
            if not items:
                print(f" 第 {page + 1} 页未找到电影列表，可能被拦截。")
                continue

            for item in items:
                try:
                    # 排名
                    rank_str = safe_extract(item.xpath('.//em/text()'))
                    if not rank_str:
                        continue
                    rank = int(rank_str)

                    # 标题
                    title = safe_extract(item.xpath('.//span[@class="title"][1]/text()'), "未知标题")
                    detail_url = safe_extract(item.xpath('.//div[@class="hd"]/a/@href'))
                    # 评分
                    rating_str = safe_extract(item.xpath('.//span[@class="rating_num"]/text()'), "0.0")
                    rating = float(rating_str)

                    # 评价人数
                    reviews = 0
                    reviews_span = item.xpath('.//span[contains(text(), "人评价")]/text()')
                    if reviews_span:
                        text = reviews_span[0]
                        # 正则表达式提取数字（可能包含逗号，如“32,797,134人评价”）
                        match = re.search(r'(\d+(?:,\d+)*)', text)
                        if match:
                            reviews = int(match.group(1).replace(',', ''))
                    # 如果没找到，尝试旧版兼容
                    if reviews == 0:
                        star_text = safe_extract(item.xpath('.//div[@class="star"]/span[last()]/text()'))
                        match = re.search(r'(\d+)', star_text)
                        reviews = int(match.group(1)) if match else 0

                    # 导演、演员、年份、国家、类型
                    info_lines = item.xpath('.//div[@class="bd"]/p[1]/text()') #第一个<p>标签
                    director, actors, year, country, genre = "", "", 0, "", ""
                    # 导演和演员（第一行）
                    if len(info_lines) >= 1:
                        line1 = info_lines[0].strip()
                        if "主演:" in line1:
                            parts = line1.split("主演:")
                            director = parts[0].replace("导演:", "").strip()
                            actors = parts[1].strip()
                        else:
                            director = line1.replace("导演:", "").strip()
                    # 年份，国家，类型（第二行）
                    if len(info_lines) >= 2:
                        line2 = info_lines[1].strip()
                        parts = [p.strip() for p in line2.split('/')]
                        if len(parts) >= 3:
                            year_match = re.search(r'\d{4}', parts[0])
                            year = int(year_match.group()) if year_match else 0
                            country = parts[1]   #国家
                            genre = parts[2]     #类型

                    # 存入电影表
                    movie_data = {
                        "movie_rank": rank, "title": title, "director": director,
                        "actors": actors, "publish_year": year, "country": country,
                        "genre": genre, "rating": rating, "reviews_count": reviews
                    }
                    helper.insert("movies_info", movie_data)

                    #  第二层：进入详情页抓取评论
                    print(f"   正在深入抓取: {rank}. {title} 的评论...")
                    time.sleep(random.uniform(2.0, 4.0))

                    try:
                        detail_resp = requests.get(detail_url, headers=get_headers(), timeout=10)
                        if detail_resp.status_code != 200:
                            print(f"    [拦截警告] 详情页被拒绝访问！状态码: {detail_resp.status_code}")
                            continue

                        detail_html = etree.HTML(detail_resp.text)
                        comments = detail_html.xpath('//div[contains(@class, "comment-item")]')[:5]

                        inserted_count = 0
                        # 解析评论
                        for c in comments:
                            commenter = safe_extract(c.xpath('.//span[@class="comment-info"]/a/text()'), "匿名")     #评论者
                            content = safe_extract(c.xpath('.//span[@class="short"]/text()'))                              #评论内容
                            content = content.replace('\n', ' ')
                            if content:
                                comment_data = {
                                    "movie_rank": rank, "commenter": commenter,
                                    "content": content
                                }
                                helper.insert("movie_comments", comment_data)
                                inserted_count += 1
                        print(f"  成功存入 {inserted_count} 条热评")

                    except Exception as e:
                        print(f"    [警告] 抓取评论失败: {e}")

                    total_movies += 1

                except Exception as e:
                    print(f"  [跳过] 解析某部电影基础信息时出错: {e}")
                    continue

            print(f" 第 {page + 1} 页列表处理完毕。")
            time.sleep(random.uniform(2.0, 4.0))       #延时控制，防封号

        except Exception as e:
            print(f" 第 {page + 1} 页整体请求失败: {e}")

    print(f"全部爬取结束，成功处理 {total_movies} 部电影及其评论。")


if __name__ == "__main__":
    db_helper = init_db()
    scrape_movie_details_and_comments(db_helper)