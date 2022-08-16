from os import environ
from typing import Awaitable

import os
import aiohttp
import asyncio
import time
import aiomultiprocess

from aiomultiprocess import Pool


from utils import chunk, fetch_list

# use fork mode for clone global variables to sub-process
aiomultiprocess.set_start_method("fork")

start = time.time()

# get src fileshare connection string from env
fileshare_info = {
    "account_name": environ.get("src_account_name", ""),
    "access_key": environ.get("src_access_key", ""),
    "fileshare_name": environ.get("src_fileshare", ""),
}
site_url = f"https://{fileshare_info['account_name']}.file.core.windows.net"

default_headers = {
    "x-ms-version": "2021-06-08",
    "Accept": "application/json",
    "User-Agent": "azsdk-python-storage-file-share/12.9.0 Python/3.9.13 (macOS-12.4-arm64-arm-64bit)",
}

params = {"comp": "list", "restype": "directory"}


async def ListDirectory(nodes: list) -> Awaitable:
    """traverse each directory with same http sessions."""
    async with aiohttp.ClientSession(site_url) as session:
        file_sets = set()
        for directory in nodes:
            file_sets |= await fetch_list(session, params, fileshare_info, directory)

    return file_sets


async def main() -> Awaitable:
    fileshare_sets = set()
    # traverse fileshare from root directory
    async with aiohttp.ClientSession(site_url) as session:
        top_directory_list = await fetch_list(session, params, fileshare_info, "")

    # remove files from root directory
    for i in top_directory_list:
        if not i.startswith("/"):
            fileshare_sets.add(i)

    top_directory_list -= fileshare_sets

    # loop top level directory with multi-process for accelate scan speed
    async with Pool() as pool:
        async for result in pool.map(
            ListDirectory, list(chunk(top_directory_list, os.cpu_count()))
        ):
            fileshare_sets |= result

    end = time.time()
    print(f"fetch from {fileshare_info['fileshare_name']}  consumes {end - start}s")
    return fileshare_sets


if __name__ == "__main__":

    # fetch source fileshare
    source_sets = asyncio.run(main())

    # get destination fileshare connection string from env
    fileshare_info = {
        "account_name": environ.get("dest_account_name", ""),
        "access_key": environ.get("dest_access_key", ""),
        "fileshare_name": environ.get("dest_fileshare", ""),
    }
    site_url = f"https://{fileshare_info['account_name']}.file.core.windows.net"

    # fetch destination fileshare
    start = time.time()
    dest_sets = asyncio.run(main())

    incremental_add = source_sets - dest_sets
    incremental_minus = dest_sets - source_sets

    print(f"incremental files nums needed to add:{len(incremental_add)}")
    print(f"incremental files nums needed to delete:{len(incremental_minus)}")
    print(incremental_add, incremental_minus)
