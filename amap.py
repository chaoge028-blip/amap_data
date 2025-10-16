import json

import requests
import xlwt

# 设置Poi搜索的各项参数
amap_api_key = '6ba90269c8b50ed3ad54d4e5fc35cfff'  # 输入自己的key
poi_search_url = 'https://restapi.amap.com/v3/place/text'

# 设置检索关键词：地区 + 公司名称关键词
region_keyword = '上海'
company_keyword = '物业公司'
search_keyword = f"{region_keyword}{company_keyword}"

# 设置爬虫网络链接测试链接
test_url = 'https://www.baidu.com'

# 设置文件输出名
file_name = f"{region_keyword}{company_keyword}.xls"

# 创建表格
workbook = xlwt.Workbook(encoding='utf-8')
worksheet = workbook.add_sheet('Sheet 1')

# 写入表头
worksheet.write(0, 0, '公司名称')
worksheet.write(0, 1, '详细地址')
worksheet.write(0, 2, '电话')

# 获取数据并保存数据
page = 1
line = 1

while page <= 100:
    params = {
        'key': amap_api_key,
        'keywords': search_keyword,
        'page': page,
        'offset': 20,
        'output': 'json',
    }
    try:
        result = requests.get(poi_search_url, params=params)
        result.raise_for_status()
        json_dict = json.loads(result.text)
        pois = json_dict.get('pois', [])

        if not pois:
            print('数据获取完成。')
            break

        for poi in pois:
            worksheet.write(line, 0, poi.get('name', ''))
            worksheet.write(line, 1, poi.get('address', ''))
            worksheet.write(line, 2, poi.get('tel', ''))
            line += 1

        print('数据正在获取中，请耐心等待。')
        page += 1
    except Exception:
        try:
            test = requests.get(test_url)
            test.raise_for_status()
            print('数据获取完成。')
            break
        except Exception:
            print('数据获取失败，请检查网络连接。')
            break

# 保存文件
workbook.save(file_name)
print('文件保存成功，请至爬虫所在目录下查看文件。')
