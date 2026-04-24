"""
豆瓣 Top100 电影特征可视化
依赖库：pandas, matplotlib, seaborn, numpy, scipy, adjustText
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import numpy as np
from collections import Counter
from scipy import stats
from mysql_helper import MySQLHelper

# 连接数据库
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
sns.set_theme(style="whitegrid", font='SimHei')


def get_db_connection():
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)['mysql']
    return MySQLHelper(**config)


db = get_db_connection()
movies_data = db.execute_query("SELECT * FROM movies_info")
df = pd.DataFrame(movies_data)       #字典转为二维表格

# 确保必要字段存在且类型正确
df['rating'] = pd.to_numeric(df['rating'], errors='coerce')         #列转为数字
df['reviews_count'] = pd.to_numeric(df['reviews_count'], errors='coerce').fillna(0)             #NAN填充为0
df['publish_year'] = pd.to_numeric(df['publish_year'], errors='coerce').fillna(0).astype(int)    #年份列转数字


# 图表1：年代分布（堆叠柱状图，按评分档次分层）
# 文件名：decade_rating_stacked.png
def plot_decade_distribution(df):
    plt.figure(figsize=(12, 6))

    # 计算年代和评分档次
    df['decade'] = (df['publish_year'] // 10) * 10

    def rating_tier(r):
        if r < 8.5:
            return '<8.5'
        elif r < 9.0:
            return '8.5-9.0'
        else:
            return '≥9.0'

    df['rating_tier'] = df['rating'].apply(rating_tier)

    # 透视表
    pivot = df.pivot_table(index='decade', columns='rating_tier', aggfunc='size', fill_value=0)
    pivot = pivot.reindex(columns=['<8.5', '8.5-9.0', '≥9.0'], fill_value=0)

    # 绘制堆叠柱状图
    pivot.plot(kind='bar', stacked=True, color=['#FF9999', '#FFD966', '#6A9C78'], edgecolor='black')
    plt.title('豆瓣 Top100 电影年代分布（按评分档次堆叠）', fontsize=14)
    plt.xlabel('年代', fontsize=12)
    plt.ylabel('电影数量', fontsize=12)
    plt.legend(title='评分区间', bbox_to_anchor=(1.05, 1))
    plt.tight_layout()
    plt.savefig('decade_rating_stacked.png', dpi=300)
    plt.close()
    print(" 图表1 已保存：decade_rating_stacked.png")


# 图表2：国家分布（水平条形图 + 平均评分标注）
# 文件名：country_barh_with_rating.png
def plot_country_distribution(df):
    plt.figure(figsize=(10, 8))

    # 提取主要国家（第一个词）
    df['main_country'] = df['country'].apply(lambda x: x.split()[0] if isinstance(x, str) else '未知')

    # 统计数量和平均评分
    country_stats = df.groupby('main_country').agg(
        count=('movie_rank', 'size'),
        avg_rating=('rating', 'mean')
    ).sort_values('count', ascending=True)

    # 取前8个国家，其余归为“其他”
    top_countries = country_stats.tail(8)
    others_count = country_stats.iloc[:-8]['count'].sum()
    others_avg = country_stats.iloc[:-8]['avg_rating'].mean() if len(country_stats) > 8 else 0
    if others_count > 0:
        top_countries.loc['其他'] = [others_count, others_avg]

    # 绘制水平条形图
    plt.barh(top_countries.index, top_countries['count'], color='#5B9BD5')
    plt.title('豆瓣 Top100 电影主要国家/地区分布', fontsize=14)
    plt.xlabel('电影数量', fontsize=12)

    # 在条形末端标注平均评分
    for i, (country, row) in enumerate(top_countries.iterrows()):
        plt.text(row['count'] + 0.5, i, f'评分 {row["avg_rating"]:.2f}', va='center', fontsize=9)

    plt.tight_layout()
    plt.savefig('country_barh_with_rating.png', dpi=300)
    plt.close()
    print(" 图表2 已保存：country_barh_with_rating.png")


# 图表3：类型分布（水平条形图 + 平均评分标注）
# 文件名：genre_frequency_with_rating.png
def plot_genre_distribution(df):
    # 类型分布：水平条形图，展示出现频次，并标注平均评分
    # 拆分类型（兼容'/'和空格分隔）
    all_genres = []
    for g in df['genre'].dropna():
        parts = str(g).replace(' ', '/').split('/')
        for part in parts:
            part = part.strip()
            if part:
                all_genres.append(part)

    genre_counts = Counter(all_genres)

    # 计算每种类型的平均评分
    genre_ratings = {}
    for genre in genre_counts.keys():
        movies_contain = df[df['genre'].str.contains(genre, na=False, regex=False)]
        if not movies_contain.empty:
            genre_ratings[genre] = movies_contain['rating'].mean()
        else:
            genre_ratings[genre] = 0

    # 转换为DataFrame并排序
    genre_df = pd.DataFrame({
        'genre': list(genre_counts.keys()),
        'count': list(genre_counts.values()),
        'avg_rating': [genre_ratings[g] for g in genre_counts.keys()]
    }).sort_values('count', ascending=False).head(12)  # 取前12种类型

    plt.figure(figsize=(10, 8))
    bars = plt.barh(genre_df['genre'], genre_df['count'], color='#5B9BD5')
    plt.title('豆瓣 Top100 电影高频类型分布（前12种）', fontsize=14)
    plt.xlabel('出现次数', fontsize=12)
    plt.ylabel('电影类型', fontsize=12)

    # 在条形末端标注平均评分（保留两位小数）
    for i, (_, row) in enumerate(genre_df.iterrows()):
        plt.text(row['count'] + 0.5, i, f'平均评分 {row["avg_rating"]:.2f}',
                 va='center', fontsize=9)

    plt.tight_layout()
    plt.savefig('genre_frequency_with_rating.png', dpi=300)
    plt.close()
    print(" 图表3 已保存：genre_frequency_with_rating.png")


# 图表4：评分 vs 评价人数（散点图 + 回归线 + 置信区间，按年代着色）
# 文件名：rating_vs_reviews_regression_by_decade.png
# ==========================================
def plot_rating_vs_reviews_enhanced(df):
    plt.figure(figsize=(10, 6))

    # 按年代着色
    df['decade'] = (df['publish_year'] // 10) * 10
    decades = sorted(df['decade'].unique())
    colors = plt.cm.tab10(np.linspace(0, 1, len(decades)))
    color_map = {dec: col for dec, col in zip(decades, colors)}

    # 评价人数转换为“万人”
    y_vals = df['reviews_count'] / 10000

    for decade in decades:
        subset = df[df['decade'] == decade]
        y_subset = subset['reviews_count'] / 10000
        plt.scatter(subset['rating'], y_subset,
                    label=f'{int(decade)}s', s=60, alpha=0.7,
                    color=color_map[decade], edgecolors='black')

    # 整体线性回归
    x = df['rating'].values
    y = y_vals.values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    x_line = np.linspace(x.min(), x.max(), 100)
    y_line = slope * x_line + intercept
    plt.plot(x_line, y_line, 'k--',
             label=f'回归线 (r={r_value:.2f}, p={p_value:.3f})')

    # 95% 置信区间
    pred_y = slope * x + intercept
    resid = y - pred_y
    std_resid = np.std(resid)
    upper = y_line + 1.96 * std_resid
    lower = y_line - 1.96 * std_resid
    plt.fill_between(x_line, lower, upper, alpha=0.2, color='gray', label='95% 置信区间')

    plt.xlabel('豆瓣评分', fontsize=12)
    plt.ylabel('评价人数（万人）', fontsize=12)
    plt.title('评分与评价人数关系（按年代着色）', fontsize=14)
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('rating_vs_reviews_regression_by_decade.png', dpi=300)
    plt.close()
    print(" 图表4 已保存：rating_vs_reviews_regression_by_decade.png")


if __name__ == "__main__":
    if df.empty:
        print(" 数据库中没有数据，请先运行爬虫。")
    else:
        print(f" 共读取 {len(df)} 部电影数据，开始生成图表...\n")
        plot_decade_distribution(df)
        plot_country_distribution(df)
        plot_genre_distribution(df)
        plot_rating_vs_reviews_enhanced(df)
        print("\n 所有图表已生成完毕，请查看当前目录下的 png 文件。")