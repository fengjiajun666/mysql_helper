import requests
from lxml import etree
import time
import random
import json
import re
from mysql_helper import MySQLHelper

# Cookie 管理模块
COOKIE_FILE = "douban_cookies.json"


def load_cookies_to_session(session):
    """从文件加载 Cookies 并正确注入到 Session 中，保留 Domain 和 Path"""
    try:
        with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
            cookies_list = json.load(f)
            for cookie in cookies_list:
                # 核心修复：使用 set 方法显式指定 domain 和 path
                session.cookies.set(
                    cookie['name'],
                    cookie['value'],
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
            print(f" 成功从文件加载了 {len(cookies_list)} 个 Cookies 到 Session")
            return True
    except FileNotFoundError:
        print(f" 未找到 Cookie 文件 {COOKIE_FILE}，请先运行 get_douban_cookies.py")
        return False


def check_cookies_valid(session):
    """通过访问电影主页并检查是否存在账号元素来验证 Cookie"""
    test_url = "https://movie.douban.com/"
    try:
        # 直接访问主页
        resp = session.get(test_url, timeout=10)

        if resp.status_code == 200:
            # 检查网页源代码中是否包含已登录的标识
            # "nav-user-account" 是豆瓣网页右上角用户头像区域的 class
            if "nav-user-account" in resp.text:
                return True
            else:
                print(" 未检测到用户账号元素，Cookie 可能已失效。")
                return False

        elif resp.status_code == 403:
            print(" 触发了豆瓣反爬 (403 Forbidden)。请稍微等待一段时间再运行。")
            return False

        else:
            print(f" 收到异常状态码: {resp.status_code}")
            return False

    except Exception as e:
        print(f" 验证 Cookie 时发生网络异常: {e}")
        return False

def refresh_cookies_automatically():
    import subprocess
    print(" Cookie 已失效，正在尝试自动刷新...")
    try:
        subprocess.run(["python", "get_douban_cookies.py"], check=True)
        print(" Cookie 刷新程序运行完毕")
        return True
    except Exception as e:
        print(f" 自动刷新失败: {e}，请手动运行 get_douban_cookies.py")
        return False


def get_valid_session():
    session = requests.Session()
    # 补充更完整的 Headers，伪装得更像真实浏览器，防止 403
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Referer': 'https://movie.douban.com/'
    })

    if not load_cookies_to_session(session):
        return None

    if check_cookies_valid(session):
        print(" Cookie 验证通过，继续爬取")
        return session
    else:
        print(" Cookie 验证失败，尝试刷新...")
        if refresh_cookies_automatically():
            # 刷新完重新加载到 session
            session.cookies.clear()
            load_cookies_to_session(session)
            if check_cookies_valid(session):
                print(" 刷新后 Cookie 有效")
                return session
        print(" 无法获得有效 Cookie，请手动检查。")
        return None

# 数据库配置与双表初始化
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
            movie_rank INT PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            director VARCHAR(100),
            actors TEXT,
            publish_year INT,
            country VARCHAR(50),
            genre VARCHAR(100),
            rating FLOAT,
            reviews_count INT
        )
    """)
    #  创建评论表（movie_comments），外键关联 movies_info
    helper.execute_non_query("""
        CREATE TABLE IF NOT EXISTS movie_comments (
            id INT PRIMARY KEY AUTO_INCREMENT,
            movie_rank INT,
            commenter VARCHAR(50),
            content TEXT,
            FOREIGN KEY (movie_rank) REFERENCES movies_info(movie_rank) ON DELETE CASCADE
        )
    """)
    # 清空旧数据
    helper.execute_non_query("TRUNCATE TABLE movie_comments")
    helper.execute_non_query("DELETE FROM movies_info")

    print(" 数据库双表准备就绪！")
    return helper

#  安全提取函数，没爬到内容则返回空字符串而不报错
def safe_extract(xpath_list, default=""):
    return xpath_list[0].strip() if xpath_list else default


#  爬虫
def scrape_movie_details_and_comments(helper, session):
    print(" 开始执行 lxml 双层深度爬取...")
    total_movies = 0

    for page in range(4):
        start = page * 25
        list_url = f'https://movie.douban.com/top250?start={start}'

        try:
            response = session.get(list_url, timeout=10)
            if "登录" in response.text or response.status_code != 200:
                print(" 检测到 Cookie 可能失效，请重新运行 get_douban_cookies.py 刷新")
                return

            html = etree.HTML(response.text)
            items = html.xpath('//div[@class="item"]')
            if not items:
                print(f" 第 {page + 1} 页未找到电影列表。")
                continue

            for item in items:
                # 排名
                rank_str = safe_extract(item.xpath('.//em/text()'))
                if not rank_str:
                    continue
                rank = int(rank_str)

                # 标题
                title = safe_extract(item.xpath('.//span[@class="title"][1]/text()'), "未知标题")

                #  详情页 URL
                detail_url = safe_extract(item.xpath('.//div[@class="hd"]/a/@href'))

                # 评分
                rating_str = safe_extract(item.xpath('.//span[@class="rating_num"]/text()'), "0.0")
                rating = float(rating_str)

                #  评价人数
                reviews = 0
                reviews_span = item.xpath('.//span[contains(text(), "人评价")]/text()')
                if reviews_span:
                    text = reviews_span[0]
                    # 正则表达式提取数字（可能包含逗号，如“32,797,134人评价”）
                    match = re.search(r'(\d+(?:,\d+)*)', text)
                    if match:
                        reviews = int(match.group(1).replace(',', ''))

                # ---- 导演、演员、年份、国家、类型 ----
                info_lines = item.xpath('.//div[@class="bd"]/p[1]/text()')           #第一个<p>标签
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
                        country = parts[1]
                        genre = parts[2]

                # 存入电影表
                movie_data = {
                    "movie_rank": rank, "title": title, "director": director,
                    "actors": actors, "publish_year": year, "country": country,
                    "genre": genre, "rating": rating, "reviews_count": reviews
                }
                helper.insert("movies_info", movie_data)

                #  第二层：进入详情页抓取评论
                print(f"  正在深入抓取: {rank}. {title} 的评论...")
                time.sleep(random.uniform(2.0, 4.0))

                try:
                    detail_resp = session.get(detail_url, timeout=10)
                    if "登录" in detail_resp.text or detail_resp.status_code != 200:
                        print(f"    [拦截] 详情页被拒绝，请检查Cookie")
                        continue

                    detail_html = etree.HTML(detail_resp.text)
                    comments = detail_html.xpath('//div[contains(@class, "comment-item")]')[:5]

                    inserted_count = 0
                    for c in comments:
                        commenter = safe_extract(c.xpath('.//span[@class="comment-info"]/a/text()'), "匿名")      #评论者
                        content = safe_extract(c.xpath('.//span[@class="short"]/text()'))                                #评论内容
                        content = content.replace('\n', ' ')
                        if content:
                            comment_data = {
                                "movie_rank": rank,
                                "commenter": commenter,
                                "content": content
                            }
                            helper.insert("movie_comments", comment_data)
                            inserted_count += 1
                    print(f"    成功存入 {inserted_count} 条热评")

                except Exception as e:
                    print(f"   [警告] 抓取评论失败: {e}")

                total_movies += 1

            print(f" 第 {page + 1} 页处理完毕。")
            time.sleep(random.uniform(3.0, 5.0))    #延时控制，防封号

        except Exception as e:
            print(f" 第 {page + 1} 页请求失败: {e}")

    print(f" 全部爬取结束！成功处理 {total_movies} 部电影及其评论。")


if __name__ == "__main__":
    db_helper = init_db()
    sess = get_valid_session()
    if sess:
        scrape_movie_details_and_comments(db_helper, sess)
    else:
        print(" 无法继续，请先获取有效 Cookie")