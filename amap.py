import json
from math import ceil
from typing import List

import requests
import xlwt

# 设置Poi搜索的各项参数
amap_api_key = '6ba90269c8b50ed3ad54d4e5fc35cfff'  # 输入自己的key
poi_search_url = 'https://restapi.amap.com/v3/place/text'
district_url = 'https://restapi.amap.com/v3/config/district'

# 设置检索关键词：地区(市) + 公司名称关键词
city_name = '上海市'
company_keyword = '物业公司'

# 单个区县最多导出的记录条数
MAX_RECORDS_PER_REGION = 2000

# 设置爬虫网络链接测试链接
test_url = 'https://www.baidu.com'


def fetch_districts(city: str) -> List[str]:
    """获取指定城市下一级的区县名称列表。"""

    params = {
        'key': amap_api_key,
        'keywords': city,
        'subdistrict': 1,
        'extensions': 'base',
    }

    try:
        response = requests.get(district_url, params=params)
        response.raise_for_status()
        data = json.loads(response.text)
    except Exception as exc:  # noqa: BLE001 - 保持简单的异常处理
        print(f'行政区划信息获取失败：{exc}')
        return []

    districts_info = data.get('districts', [])
    if not districts_info:
        return []

    sub_districts = districts_info[0].get('districts', [])
    district_names = [item.get('name') for item in sub_districts if item.get('name')]
    return district_names


def export_pois_for_region(region: str) -> None:
    """按区县导出Poi信息。"""

    search_keyword = f"{region}{company_keyword}"
    file_name = f"{city_name}{region}{company_keyword}.xls"

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Sheet 1')

    worksheet.write(0, 0, '公司名称')
    worksheet.write(0, 1, '详细地址')
    worksheet.write(0, 2, '电话')

    page = 1
    line = 1
    total_pages = None
    records_written = 0

    while True:
        params = {
            'key': amap_api_key,
            'keywords': search_keyword,
            'city': city_name,
            'page': page,
            'offset': 20,
            'output': 'json',
        }
        try:
            result = requests.get(poi_search_url, params=params)
            result.raise_for_status()
            json_dict = json.loads(result.text)
            pois = json_dict.get('pois', [])

            if total_pages is None:
                try:
                    total_count = int(json_dict.get('count', '0') or 0)
                except ValueError:
                    total_count = 0
                if total_count > 0:
                    total_count = min(total_count, MAX_RECORDS_PER_REGION)
                    total_pages = ceil(total_count / params['offset'])

            if not pois:
                if page == 1:
                    print(f'{region}暂无更多数据。')
                else:
                    print(f'{region}数据获取完成。')
                break

            for poi in pois:
                if records_written >= MAX_RECORDS_PER_REGION:
                    break
                worksheet.write(line, 0, poi.get('name', ''))
                worksheet.write(line, 1, poi.get('address', ''))
                worksheet.write(line, 2, poi.get('tel', ''))
                line += 1
                records_written += 1

            if records_written >= MAX_RECORDS_PER_REGION:
                print(f'{region}已达到{MAX_RECORDS_PER_REGION}条上限，停止继续获取。')
                break
            print(f'{region}数据正在获取中，请耐心等待。')
            page += 1
            if total_pages is not None and page > total_pages:
                print(f'{region}数据获取完成。')
                break
        except Exception:
            try:
                test = requests.get(test_url)
                test.raise_for_status()
                print(f'{region}数据获取完成。')
                break
            except Exception:
                print(f'{region}数据获取失败，请检查网络连接。')
                break

    workbook.save(file_name)
    print(f'{region}文件保存成功：{file_name}')


def main() -> None:
    districts = fetch_districts(city_name)
    if not districts:
        print('未获取到区县信息，请检查城市名称或网络连接。')
        return

    for district in districts:
        export_pois_for_region(district)

    print('所有区县的数据处理完毕。')


if __name__ == '__main__':
    main()
