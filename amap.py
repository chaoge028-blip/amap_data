import json
import random
import time
from typing import Dict, List

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

# 请求频率及限流重试相关配置
BASE_REQUEST_INTERVAL = 0.2  # 每次请求后的基础等待时间（秒）
RATE_LIMIT_BASE_DELAY = 1.5  # 首次遭遇限流后的等待时间（秒）
MAX_RATE_LIMIT_DELAY = 60    # 限流情况下的最长等待时间（秒）
MAX_RATE_LIMIT_RETRIES = 6   # 单页允许的限流重试次数

# 设置爬虫网络链接测试链接
test_url = 'https://www.baidu.com'


def fetch_districts(city: str) -> List[Dict[str, str]]:
    """获取指定城市下一级的区县名称列表及其adcode。"""

    params = {
        'key': amap_api_key,
        'keywords': city,
        'subdistrict': 1,
        'extensions': 'all',
    }

    try:
        response = requests.get(district_url, params=params)
        response.raise_for_status()
        data = json.loads(response.text)
    except Exception as exc:  # noqa: BLE001 - 保持简单的异常处理
        print(f'行政区划信息获取失败：{exc}')
        return []

    if data.get('status') != '1':
        info = data.get('info', '未知错误')
        infocode = data.get('infocode', '')
        message = f'行政区划信息获取失败：{info}'
        if infocode:
            message += f'（代码：{infocode}）'
        print(message)
        return []

    districts_info = data.get('districts', [])
    if not districts_info:
        return []

    sub_districts = districts_info[0].get('districts', [])
    districts: List[Dict[str, str]] = []
    for item in sub_districts:
        name = item.get('name')
        adcode = item.get('adcode')
        if name and adcode:
            districts.append({'name': name, 'adcode': adcode})
    return districts


def export_pois_for_region(region_name: str, region_adcode: str) -> None:
    """按区县导出Poi信息。"""

    file_name = f"{city_name}{region_name}{company_keyword}.xls"

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Sheet 1')

    worksheet.write(0, 0, '公司名称')
    worksheet.write(0, 1, '详细地址')
    worksheet.write(0, 2, '电话')

    page = 1
    line = 1
    records_written = 0
    consecutive_errors = 0
    rate_limit_attempts = 0

    while True:
        params = {
            'key': amap_api_key,
            'keywords': company_keyword,
            'city': region_adcode,
            'citylimit': 'true',
            'page': page,
            'offset': 20,
            'output': 'json',
        }
        try:
            result = requests.get(poi_search_url, params=params)
            result.raise_for_status()
            json_dict = json.loads(result.text)
            status = json_dict.get('status')
            if status != '1':
                info = json_dict.get('info', '未知错误')
                infocode = json_dict.get('infocode', '')
                info_text = str(info)
                info_upper = info_text.upper()
                infocode_text = f'（代码：{infocode}）' if infocode else ''

                if 'INVALID_PAGE' in info_upper:
                    print(f'{region_name}已无更多数据，停止在第{page}页。{info_text}{infocode_text}')
                    break

                if 'OVER_LIMIT' in info_upper or 'FREQUENT' in info_upper:
                    if rate_limit_attempts < MAX_RATE_LIMIT_RETRIES:
                        sleep_seconds = min(
                            RATE_LIMIT_BASE_DELAY * (2 ** rate_limit_attempts),
                            MAX_RATE_LIMIT_DELAY,
                        )
                        jitter = random.uniform(0, 0.5 * sleep_seconds)
                        wait_time = sleep_seconds + jitter
                        rate_limit_attempts += 1
                        print(
                            f'{region_name}请求受限：{info_text}{infocode_text}，'
                            f'第{rate_limit_attempts}次限流重试，等待{wait_time:.1f}秒。'
                        )
                        time.sleep(wait_time)
                        continue

                    print(
                        f'{region_name}频率受限达到最大重试次数：{info_text}{infocode_text}，停止获取。'
                    )
                    break

                if consecutive_errors < 2:
                    consecutive_errors += 1
                    print(f'{region_name}第{page}页请求失败：{info_text}{infocode_text}，第{consecutive_errors}次重试。')
                    time.sleep(1)
                    continue

                print(f'{region_name}连续多次请求失败：{info_text}{infocode_text}，停止获取。')
                break

            consecutive_errors = 0
            rate_limit_attempts = 0
            pois = json_dict.get('pois', [])

            if not pois:
                if page == 1:
                    print(f'{region_name}暂无更多数据。')
                else:
                    print(f'{region_name}数据获取完成。')
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
                print(f'{region_name}已达到{MAX_RECORDS_PER_REGION}条上限，停止继续获取。')
                break
            if len(pois) < params['offset']:
                print(f'{region_name}数据获取完成。')
                break

            print(f'{region_name}数据正在获取中，请耐心等待。')
            page += 1
            time.sleep(BASE_REQUEST_INTERVAL)
        except Exception:
            try:
                test = requests.get(test_url)
                test.raise_for_status()
                print(f'{region_name}数据获取完成。')
                break
            except Exception:
                print(f'{region_name}数据获取失败，请检查网络连接。')
                break

    workbook.save(file_name)
    print(f'{region_name}文件保存成功：{file_name}')


def main() -> None:
    districts = fetch_districts(city_name)
    if not districts:
        print('未获取到区县信息，请检查城市名称或网络连接。')
        return

    for district in districts:
        export_pois_for_region(district['name'], district['adcode'])

    print('所有区县的数据处理完毕。')


if __name__ == '__main__':
    main()
