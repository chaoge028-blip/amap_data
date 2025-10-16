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

# 单个区县最多导出的记录条数（高德官方上限：25 条 × 100 页 = 2500 条）
MAX_RECORDS_PER_REGION = 2500

# 每页请求的最大条目数（官方限制）
PAGE_SIZE = 25

# 官方允许的最大翻页次数
MAX_PAGES_PER_REGION = 100

# 请求频率及限流重试相关配置
BASE_REQUEST_INTERVAL = 0.2   # 每次请求后的基础等待时间（秒）
RATE_LIMIT_BASE_DELAY = 1.5   # 首次遭遇限流后的等待时间（秒）
MAX_RATE_LIMIT_DELAY = 60     # 限流情况下的最长等待时间（秒）
MAX_RATE_LIMIT_RETRIES = 6    # 单页允许的限流重试次数

# 网络错误重试配置
NETWORK_RETRY_BASE_DELAY = 1.0  # 首次网络异常后的等待时间（秒）
MAX_NETWORK_RETRY_DELAY = 20    # 网络异常时的最大等待时间（秒）
MAX_NETWORK_RETRIES = 4         # 网络异常的最大重试次数
REQUEST_TIMEOUT = 10            # 单次请求的超时时间（秒）

# 复用会话以提升网络请求稳定性
session = requests.Session()


def request_json_with_retry(url: str, params: Dict[str, str], context: str) -> Dict:
    """带有限次重试机制的 JSON 请求工具。"""

    attempt = 0
    while True:
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return json.loads(response.text)
        except (requests.RequestException, json.JSONDecodeError) as exc:
            if attempt >= MAX_NETWORK_RETRIES:
                raise RuntimeError(f'{context}请求失败：{exc}') from exc

            sleep_seconds = min(
                NETWORK_RETRY_BASE_DELAY * (2 ** attempt),
                MAX_NETWORK_RETRY_DELAY,
            )
            jitter = random.uniform(0, 0.5 * sleep_seconds)
            wait_time = sleep_seconds + jitter
            attempt += 1
            print(
                f'{context}请求异常：{exc}，第{attempt}次网络重试，'
                f'等待{wait_time:.1f}秒后继续。'
            )
            time.sleep(wait_time)


def fetch_districts(city: str) -> List[Dict[str, str]]:
    """获取指定城市下一级的区县名称列表及其adcode。"""

    params = {
        'key': amap_api_key,
        'keywords': city,
        'subdistrict': 1,
        'extensions': 'all',
    }

    try:
        data = request_json_with_retry(district_url, params, f'{city}行政区划')
    except RuntimeError as exc:
        print(exc)
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
    declared_total = None
    declared_total_logged = False

    while True:
        params = {
            'key': amap_api_key,
            'keywords': company_keyword,
            'city': region_adcode,
            'citylimit': 'true',
            'page': page,
            'offset': PAGE_SIZE,
            'output': 'json',
        }
        context = f'{region_name}第{page}页'
        if page > MAX_PAGES_PER_REGION:
            extra = ''
            if declared_total is not None:
                extra = f'（接口提示总量约{declared_total}条。）'
            print(
                f'{region_name}已达到高德允许的最大翻页次数（{MAX_PAGES_PER_REGION}页），'
                f'共写入{records_written}条。{extra}'
            )
            break
        try:
            json_dict = request_json_with_retry(poi_search_url, params, context)
        except RuntimeError as exc:
            print(exc)
            break

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

        if declared_total is None:
            count_str = json_dict.get('count')
            try:
                declared_total = int(count_str)
            except (TypeError, ValueError):
                declared_total = None

        if declared_total is not None and not declared_total_logged:
            print(
                f'{region_name}高德接口提示的可用数据量约为{declared_total}条，'
                '实际获取数量可能受关键词覆盖范围或接口限制影响。'
            )
            declared_total_logged = True

        pois = json_dict.get('pois', [])

        if not pois:
            if page == 1:
                message = f'{region_name}暂无更多数据。'
                if declared_total is not None:
                    message += f'（接口提示总量约{declared_total}条。）'
                print(message)
            else:
                message = f'{region_name}数据获取完成。共写入{records_written}条。'
                if declared_total is not None:
                    message += f'（接口提示总量约{declared_total}条。）'
                print(message)
            break

        page_records = 0
        for poi in pois:
            if records_written >= MAX_RECORDS_PER_REGION:
                break
            worksheet.write(line, 0, poi.get('name', ''))
            worksheet.write(line, 1, poi.get('address', ''))
            worksheet.write(line, 2, poi.get('tel', ''))
            line += 1
            records_written += 1
            page_records += 1

        if page_records:
            print(f'{region_name}第{page}页写入{page_records}条，累计{records_written}条。')

        if records_written >= MAX_RECORDS_PER_REGION:
            extra = ''
            if (
                declared_total is not None
                and declared_total > MAX_RECORDS_PER_REGION
            ):
                extra = (
                    f'（接口预估总量约为{declared_total}条，已触及{MAX_RECORDS_PER_REGION}条安全上限。）'
                )
            print(f'{region_name}已达到{MAX_RECORDS_PER_REGION}条上限，停止继续获取。{extra}')
            break
        if len(pois) < params['offset']:
            summary = f'{region_name}数据获取完成。共写入{records_written}条。'
            if declared_total is not None:
                summary += f'（接口提示总量约{declared_total}条。）'
            print(summary)
            break

        print(f'{region_name}数据正在获取中，请耐心等待。')
        page += 1
        time.sleep(BASE_REQUEST_INTERVAL)

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
