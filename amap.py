import json
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import xlwt

# 设置Poi搜索的各项参数
amap_api_key = '6ba90269c8b50ed3ad54d4e5fc35cfff'  # 输入自己的key
polygon_search_url = 'https://restapi.amap.com/v3/place/polygon'
district_url = 'https://restapi.amap.com/v3/config/district'

# 设置检索关键词：地区(市) + 公司名称关键词
city_name = '成都市'
company_keyword = '广告'

# 单个区县允许导出的最大记录条数（可根据需要调整，None 表示不设上限）
MAX_RECORDS_PER_REGION: Optional[int] = None

# 若仅需导出指定区县，可在此列出目标名称或 adcode；为空列表时默认导出城市下全部区县
TARGET_DISTRICTS: List[str] = []

# 每页请求的最大条目数（官方限制）
PAGE_SIZE = 25

# 官方允许的最大翻页次数
MAX_PAGES_PER_REGION = 100

# 子区域切片策略
MIN_CELL_EDGE_DEGREES = 0.01   # 单个小网格的最小经纬度边长
MAX_CELL_SPLIT_DEPTH = 6       # 最深递归拆分层数（4^6 = 4096 个子区域）

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

# 调试日志文件，用于记录导致数据提前终止的接口返回信息
DEBUG_LOG_FILE = 'amap_debug.log'

# 复用会话以提升网络请求稳定性
session = requests.Session()


@dataclass
class BoundingBox:
    min_lng: float
    min_lat: float
    max_lng: float
    max_lat: float

    def width(self) -> float:
        return self.max_lng - self.min_lng

    def height(self) -> float:
        return self.max_lat - self.min_lat

    def can_split(self) -> bool:
        return self.width() > MIN_CELL_EDGE_DEGREES and self.height() > MIN_CELL_EDGE_DEGREES

    def split(self) -> Tuple['BoundingBox', 'BoundingBox', 'BoundingBox', 'BoundingBox']:
        mid_lng = (self.min_lng + self.max_lng) / 2
        mid_lat = (self.min_lat + self.max_lat) / 2
        return (
            BoundingBox(self.min_lng, self.min_lat, mid_lng, mid_lat),
            BoundingBox(mid_lng, self.min_lat, self.max_lng, mid_lat),
            BoundingBox(self.min_lng, mid_lat, mid_lng, self.max_lat),
            BoundingBox(mid_lng, mid_lat, self.max_lng, self.max_lat),
        )


def parse_district_bbox(polyline: Optional[str]) -> Optional[BoundingBox]:
    if not polyline:
        return None

    min_lng = float('inf')
    min_lat = float('inf')
    max_lng = float('-inf')
    max_lat = float('-inf')

    for segment in polyline.split('|'):
        for pair in segment.split(';'):
            if not pair:
                continue
            parts = pair.split(',')
            if len(parts) != 2:
                continue
            try:
                lng = float(parts[0])
                lat = float(parts[1])
            except ValueError:
                continue
            min_lng = min(min_lng, lng)
            min_lat = min(min_lat, lat)
            max_lng = max(max_lng, lng)
            max_lat = max(max_lat, lat)

    if min_lng == float('inf') or min_lat == float('inf'):
        return None

    return BoundingBox(min_lng=min_lng, min_lat=min_lat, max_lng=max_lng, max_lat=max_lat)


def bbox_to_polygon_string(bbox: BoundingBox) -> str:
    return (
        f'{bbox.min_lng:.6f},{bbox.min_lat:.6f};'
        f'{bbox.max_lng:.6f},{bbox.min_lat:.6f};'
        f'{bbox.max_lng:.6f},{bbox.max_lat:.6f};'
        f'{bbox.min_lng:.6f},{bbox.max_lat:.6f};'
        f'{bbox.min_lng:.6f},{bbox.min_lat:.6f}'
    )


def make_poi_unique_key(poi: Dict[str, str]) -> Tuple[str, ...]:
    poi_id = poi.get('id')
    if poi_id:
        return ('id', poi_id)
    name = poi.get('name', '').strip()
    address = poi.get('address', '').strip()
    location = poi.get('location', '').strip()
    return ('fallback', name, address, location)


def log_debug(message: str) -> None:
    """将调试信息追加写入日志文件。"""

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with open(DEBUG_LOG_FILE, 'a', encoding='utf-8') as log_file:
            log_file.write(f'[{timestamp}] {message}\n')
    except OSError as exc:
        print(f'写入调试日志失败：{exc}')


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
            message = (
                f'{context}请求异常：{exc}，第{attempt}次网络重试，等待{wait_time:.1f}秒后继续。'
            )
            print(message)
            log_debug(message)
            time.sleep(wait_time)


def fetch_single_district_details(keyword: str, label: str) -> Optional[Dict[str, Any]]:
    """按关键字单独查询行政区划信息，用于补充缺失的边界框或独立检索。"""

    params = {
        'key': amap_api_key,
        'keywords': keyword,
        'subdistrict': 0,
        'extensions': 'all',
    }

    try:
        data = request_json_with_retry(district_url, params, f'{label}行政区信息补全')
    except RuntimeError as exc:
        message = f'{label}行政区信息补全失败：{exc}'
        print(message)
        log_debug(message)
        return None

    if data.get('status') != '1':
        info = data.get('info', '未知错误')
        infocode = data.get('infocode', '')
        message = f'{label}行政区信息补全失败：{info}'
        if infocode:
            message += f'（代码：{infocode}）'
        print(message)
        log_debug(message)
        return None

    for district in data.get('districts', []):
        bbox = parse_district_bbox(district.get('polyline'))
        name = district.get('name') or label
        adcode = district.get('adcode')
        if adcode:
            if bbox is None:
                log_debug(f'{name}行政区信息补全缺少边界数据，后续检索可能无法拆分网格。')
            return {'name': name, 'adcode': adcode, 'bbox': bbox}

    return None


def fetch_districts(city: str) -> List[Dict[str, Any]]:
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
        log_debug(f'{city}行政区划请求失败：{exc}')
        return []

    if data.get('status') != '1':
        info = data.get('info', '未知错误')
        infocode = data.get('infocode', '')
        message = f'行政区划信息获取失败：{info}'
        if infocode:
            message += f'（代码：{infocode}）'
        print(message)
        log_debug(message)
        return []

    districts_info = data.get('districts', [])
    if not districts_info:
        return []

    sub_districts = districts_info[0].get('districts', [])
    districts: List[Dict[str, Any]] = []
    for item in sub_districts:
        name = item.get('name')
        adcode = item.get('adcode')
        bbox = parse_district_bbox(item.get('polyline'))

        if not bbox and adcode:
            details = fetch_single_district_details(adcode, f'{name or adcode}(adcode)')
            if details:
                bbox = details.get('bbox')
                name = name or details.get('name')
                adcode = details.get('adcode', adcode)

        if not bbox and name:
            details = fetch_single_district_details(name, f'{name}')
            if details:
                bbox = details.get('bbox')
                adcode = details.get('adcode', adcode)

        if name and adcode:
            if bbox is None:
                log_debug(f'{name}仍缺少有效边界信息，后续检索将无法进行网格拆分。')
            districts.append({'name': name, 'adcode': adcode, 'bbox': bbox})

    return districts


def fetch_pois_for_polygon(
    region_name: str,
    region_adcode: str,
    cell_index: int,
    bbox: BoundingBox,
) -> Tuple[List[Dict[str, Any]], Optional[int], bool]:
    polygon_param = bbox_to_polygon_string(bbox)
    cell_label = f'{region_name}子区域{cell_index}'
    page = 1
    consecutive_errors = 0
    rate_limit_attempts = 0
    declared_total: Optional[int] = None
    limit_hit = False
    collected: List[Dict[str, Any]] = []
    declared_total_announced = False

    while True:
        if page > MAX_PAGES_PER_REGION:
            limit_hit = True
            message = (
                f'{cell_label}已达到高德允许的最大翻页次数（{MAX_PAGES_PER_REGION}页）。'
            )
            print(message)
            log_debug(message)
            break

        params = {
            'key': amap_api_key,
            'keywords': company_keyword,
            'city': region_adcode,
            'polygon': polygon_param,
            'page': page,
            'offset': PAGE_SIZE,
            'output': 'json',
        }

        context = f'{cell_label}第{page}页'
        try:
            json_dict = request_json_with_retry(polygon_search_url, params, context)
        except RuntimeError as exc:
            message = f'{context}请求失败：{exc}'
            print(message)
            log_debug(message)
            break

        status = json_dict.get('status')
        if status != '1':
            info = json_dict.get('info', '未知错误')
            infocode = json_dict.get('infocode', '')
            info_text = str(info)
            infocode_text = f'（代码：{infocode}）' if infocode else ''
            info_upper = info_text.upper()

            if 'INVALID_PAGE' in info_upper:
                message = f'{cell_label}提示无更多数据，停止在第{page}页。{info_text}{infocode_text}'
                print(message)
                log_debug(message)
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
                    message = (
                        f'{cell_label}请求受限：{info_text}{infocode_text}，第{rate_limit_attempts}次限流重试，等待{wait_time:.1f}秒。'
                    )
                    print(message)
                    log_debug(message)
                    time.sleep(wait_time)
                    continue

                message = f'{cell_label}频率受限达到最大重试次数：{info_text}{infocode_text}，停止获取。'
                print(message)
                log_debug(message)
                limit_hit = True
                break

            if consecutive_errors < 2:
                consecutive_errors += 1
                message = (
                    f'{cell_label}请求失败：{info_text}{infocode_text}，第{consecutive_errors}次重试。'
                )
                print(message)
                log_debug(message)
                time.sleep(1)
                continue

            message = f'{cell_label}连续多次请求失败：{info_text}{infocode_text}，停止获取。'
            print(message)
            log_debug(message)
            break

        consecutive_errors = 0
        rate_limit_attempts = 0

        if declared_total is None:
            count_str = json_dict.get('count')
            try:
                declared_total = int(count_str)
            except (TypeError, ValueError):
                declared_total = None
        if declared_total is not None and not declared_total_announced:
            message = (
                f'{cell_label}接口提示的潜在数据量约为{declared_total}条，实际可获取量可能受关键词与区域切分影响。'
            )
            print(message)
            log_debug(message)
            declared_total_announced = True

        pois = json_dict.get('pois', [])
        if not pois:
            if declared_total is not None and declared_total > len(collected):
                limit_hit = True
                payload = json.dumps(json_dict, ensure_ascii=False)
                log_debug(f'{context}返回空数据但声明数量更大，响应：{payload}')
            break

        print(f'{cell_label}第{page}页获取{len(pois)}条数据。')
        collected.extend(pois)

        if len(pois) < PAGE_SIZE:
            break

        page += 1
        time.sleep(BASE_REQUEST_INTERVAL)

    if declared_total is not None and declared_total > len(collected):
        limit_hit = True
        message = (
            f'{cell_label}累计获取{len(collected)}条，接口提示总量约{declared_total}条，可能仍有剩余数据。'
        )
        log_debug(message)

    if len(collected) >= MAX_PAGES_PER_REGION * PAGE_SIZE:
        limit_hit = True

    return collected, declared_total, limit_hit


def export_pois_for_region(region_name: str, region_adcode: str, bbox: Optional[BoundingBox]) -> None:
    """按区县导出Poi信息，必要时拆分子区域以规避单次查询上限。"""

    if bbox is None:
        message = f'{region_name}缺少有效边界信息，暂无法按网格拆分检索。'
        print(message)
        log_debug(message)
        return

    file_name = f"{city_name}{region_name}{company_keyword}.xls"

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Sheet 1')

    worksheet.write(0, 0, '公司名称')
    worksheet.write(0, 1, '详细地址')
    worksheet.write(0, 2, '电话')

    line = 1
    total_written = 0
    seen_keys: Set[Tuple[str, ...]] = set()
    cells = deque([(bbox, 0)])
    cell_counter = 0

    while cells:
        cell_bbox, depth = cells.popleft()
        cell_counter += 1
        cell_label = f'{region_name}子区域{cell_counter}'

        pois, _declared_total, limit_hit = fetch_pois_for_polygon(
            region_name,
            region_adcode,
            cell_counter,
            cell_bbox,
        )

        new_records = 0
        for poi in pois:
            key = make_poi_unique_key(poi)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            worksheet.write(line, 0, poi.get('name', ''))
            worksheet.write(line, 1, poi.get('address', ''))
            worksheet.write(line, 2, poi.get('tel', ''))
            line += 1
            total_written += 1
            new_records += 1

            if MAX_RECORDS_PER_REGION is not None and total_written >= MAX_RECORDS_PER_REGION:
                break

        print(f'{cell_label}写入{new_records}条，累计{total_written}条。')

        if MAX_RECORDS_PER_REGION is not None and total_written >= MAX_RECORDS_PER_REGION:
            message = (
                f'{region_name}已达到配置的导出上限（{MAX_RECORDS_PER_REGION}条），提前结束后续子区域抓取。'
            )
            print(message)
            log_debug(message)
            break

        if limit_hit and depth < MAX_CELL_SPLIT_DEPTH and cell_bbox.can_split():
            children = cell_bbox.split()
            for child in children:
                cells.append((child, depth + 1))
            split_msg = (
                f'{cell_label}结果仍接近接口上限，拆分为{len(children)}个更小子区域继续检索。'
            )
            print(split_msg)
            log_debug(split_msg)
        elif limit_hit:
            msg = (
                f'{cell_label}已达到接口限制，但区域过小或拆分层级过深（当前深度{depth}），无法继续细分。'
            )
            print(msg)
            log_debug(msg)

    workbook.save(file_name)
    summary = f'{region_name}数据导出完成，共写入{total_written}条。文件保存为：{file_name}'
    print(summary)
    log_debug(summary)


def main() -> None:
    districts = fetch_districts(city_name)
    if not districts:
        print('未获取到区县信息，请检查城市名称或网络连接。')
        return

    if TARGET_DISTRICTS:
        desired: List[Dict[str, Any]] = []
        seen_codes: Set[str] = set()
        name_map = {item['name']: item for item in districts if item.get('name')}
        code_map = {item['adcode']: item for item in districts if item.get('adcode')}

        for target in TARGET_DISTRICTS:
            keyword = target.strip()
            if not keyword:
                continue

            candidate = name_map.get(keyword) or code_map.get(keyword)
            if candidate:
                adcode = candidate['adcode']
                if adcode in seen_codes:
                    continue
                bbox = candidate.get('bbox')
                if bbox is None:
                    details = fetch_single_district_details(adcode, f"{candidate['name']}(adcode)")
                    if not details and candidate.get('name'):
                        details = fetch_single_district_details(candidate['name'], candidate['name'])
                    if details:
                        bbox = details.get('bbox')
                seen_codes.add(adcode)
                desired.append({
                    'name': candidate['name'],
                    'adcode': adcode,
                    'bbox': bbox,
                })
                continue

            details = fetch_single_district_details(keyword, keyword)
            if details:
                adcode = details['adcode']
                if adcode in seen_codes:
                    continue
                seen_codes.add(adcode)
                desired.append(details)
            else:
                message = f'未能获取{keyword}的行政区信息，请确认名称或adcode是否正确。'
                print(message)
                log_debug(message)

        if not desired:
            print('未匹配到任何目标区县，程序结束。')
            return

        districts = desired

    for district in districts:
        export_pois_for_region(district['name'], district['adcode'], district.get('bbox'))

    print('所有区县的数据处理完毕。')


if __name__ == '__main__':
    main()
