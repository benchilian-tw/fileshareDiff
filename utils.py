from typing import Awaitable, Iterable
import uuid
import time
import six
import base64
import hashlib
import hmac
import aiohttp
import xml.etree.ElementTree as ET
from itertools import islice

# Weekday and month names for HTTP date/time formatting; always English!
_weekdayname = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_monthname = [
    None,  # Dummy so we can use 1-based month numbers
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

a = "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-client-request-id:39d4de1c-1958-11ed-85ae-7a8abb192e12\nx-ms-date:Thu, 11 Aug 2022 09:30:23 GMT\nx-ms-version:2021-06-08\n/test2azure/adfsource/03120ebd-54b8-4f10-bdb4-40a840b7529b\ncomp:list\nrestype:directory"

_default_headers = {
    "x-ms-version": "2021-06-08",
    "Accept": "application/json",
    "User-Agent": "azsdk-python-storage-file-share/12.9.0 Python/3.9.13 (macOS-12.4-arm64-arm-64bit)",
}

_params = {"restype": "directory", "comp": "list"}


def format_date_time(timestamp: float) -> str:
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    return "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
        _weekdayname[wd],
        day,
        _monthname[month],
        year,
        hh,
        mm,
        ss,
    )


def encode_base64(data: bytes) -> str:
    if isinstance(data, six.text_type):
        data = data.encode("utf-8")
    encoded = base64.b64encode(data)
    return encoded.decode("utf-8")


def sign_string(key: str, string_to_sign: str) -> str:
    key = base64.b64decode(key.encode("utf-8"))
    string_to_sign = string_to_sign.encode("utf-8")
    signed_hmac_sha256 = hmac.HMAC(key, string_to_sign, hashlib.sha256)
    digest = signed_hmac_sha256.digest()
    encoded_digest = encode_base64(digest)
    return encoded_digest


def gen_auth_header(access_key: str, account_name: str, string_to_sign: str) -> str:
    signature = sign_string(access_key, string_to_sign)
    auth_string = "SharedKey " + account_name + ":" + signature
    return auth_string


def gen_headers(access_key: str, account_name: str, uri: str, params: dict) -> dict:
    request_id = str(uuid.uuid1())
    current_time = format_date_time(time.time())

    param_string = ""

    # append params by alphabet sorted
    for param, value in sorted(params.items()):
        param_string += f"\n{param}:{value}"

    string_to_sign = f"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-client-request-id:{request_id}\nx-ms-date:{current_time}\nx-ms-version:2021-06-08\n{uri}{param_string}"
    auth_string = gen_auth_header(access_key, account_name, string_to_sign)

    return {
        "x-ms-client-request-id": request_id,
        "x-ms-date": current_time,
        "Authorization": auth_string,
    }


def parse_response(res: str) -> dict:
    root = ET.fromstring(res)

    return {
        "dirs": root.findall("./Entries/Directory/Name"),
        "files": root.findall("./Entries/File/Name"),
        "marker": root[-1].text,
    }


def chunk(it: Iterable, size: int):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def fetch_list(
    session: aiohttp.ClientSession, params: dict, fileshare_info: dict, directory: str
) -> Awaitable:
    """return the sets of specific directory and it's sub-directory"""
    file_sets = set()

    # root directory and sub-level directory has different conversion of name
    if directory:
        endpoint = f"/{fileshare_info['account_name']}/{fileshare_info['fileshare_name']}{directory}"
        uri = f"/{fileshare_info['fileshare_name']}{directory}"
    else:
        endpoint = (
            f"/{fileshare_info['account_name']}/{fileshare_info['fileshare_name']}"
        )
        uri = f"/{fileshare_info['fileshare_name']}"

    # generate http headers
    headers = gen_headers(
        fileshare_info["access_key"], fileshare_info["account_name"], endpoint, params
    )

    async with session.get(
        uri,
        params=params,
        headers={**headers, **_default_headers},
    ) as resp:
        content = await resp.text()
        data = parse_response(content)

        # for root level directory, we want fetch a list of directory for futher process
        if not directory:
            for d in data["dirs"]:
                file_sets.add(f"/{d.text}")

            for f in data["files"]:
                file_sets.add(f.text)

        # for sub level directory, traverse the directory to get a list of fqdn file path
        else:
            for f in data["files"]:
                file_sets.add(f"{directory}/{f.text}")

            for d in data["dirs"]:
                file_sets |= await fetch_list(
                    session,
                    params,
                    fileshare_info,
                    directory + f"/{d.text}",
                )

        # azure storage rest-api only return max 5000 results, so we loop over all slice by marker flag
        if data["marker"]:
            slice_sets = await fetch_list(
                session, {**params, "marker": data["marker"]}, fileshare_info, directory
            )
            file_sets |= slice_sets

    return file_sets
