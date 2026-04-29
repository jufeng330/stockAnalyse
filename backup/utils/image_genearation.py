import argparse
import asyncio
import contextlib
import json
import os
import socket
import sys
import time
from functools import partial
from typing import Dict
from typing import List
from typing import Union

import httpx
import regex
import requests

import os
import logging


def get_proxy(proxy: str = None):
    """
    获取系统设置的代理。

    函数将尝试从提供的参数或环境变量中读取代理设置。
    支持的环境变量包括 all_proxy, ALL_PROXY, http_proxy, HTTP_PROXY, https_proxy, HTTPS_PROXY。
    对于不符合标准格式的代理地址，函数将进行适当的格式转换或抛出异常。

    :param proxy: 可选，显式指定的代理字符串。
    :return: 格式化后的代理字符串或None（如果未找到合适的代理设置）。
    """
    try:
        proxy = (
                proxy
                or os.environ.get("all_proxy")
                or os.environ.get("ALL_PROXY")
                or os.environ.get("http_proxy")
                or os.environ.get("HTTP_PROXY")
                or os.environ.get("https_proxy")
                or os.environ.get("HTTPS_PROXY")
        )

        if proxy:
            if proxy.startswith("socks5h://"):
                proxy = "socks5://" + proxy[len("socks5h://"):]
            elif not (proxy.startswith("http://") or proxy.startswith("https://") or proxy.startswith("socks5://")):
                raise ValueError(f"Unsupported proxy format: {proxy}")
            return proxy
        else:
            # logging.warning("No proxy configuration found in environment variables.")
            return None
    except Exception as e:
        logging.error(f"Error getting proxy: {e}")
        raise


take_ip_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
take_ip_socket.connect(("8.8.8.8", 80))
FORWARDED_IP: str = take_ip_socket.getsockname()[0]
take_ip_socket.close()

BING_URL = os.getenv("BING_URL", "https://www.bing.com")

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.77",
    "accept-language": "en,zh-TW;q=0.9,zh;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "referrer": "https://www.bing.com/images/create/",
    "origin": "https://www.bing.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/119.0.0.0 "
                  "Safari/537.36 "
                  "Edg/119.0.0.0",
    "x-forwarded-for": FORWARDED_IP,
}

# Error messages
error_timeout = "Your request has timed out."
error_redirect = "Redirect failed"
error_blocked_prompt = (
    "Your prompt has been blocked by Bing. Try to change any bad words and try again."
)
error_being_reviewed_prompt = "Your prompt is being reviewed by Bing. Try to change any sensitive words and try again."
error_noresults = "Could not get results"
error_unsupported_lang = "\nthis language is currently not supported by bing"
error_bad_images = "Bad images"
error_no_images = "No images"
# Action messages
sending_message = "Sending request..."
wait_message = "Waiting for results..."
download_message = "\nDownloading images..."


def debug(debug_file, text_var):
    """helper function for debug"""
    with open(f"{debug_file}", "a", encoding="utf-8") as f:
        f.write(str(text_var))
        f.write("\n")


class ImageGen:
    """
    Image generation by Microsoft Bing
    Parameters:
        auth_cookie: str
    Optional Parameters:
        debug_file: str
        quiet: bool
        all_cookies: List[Dict]
    """

    def __init__(
            self,
            auth_cookie: str,
            debug_file: Union[str, None] = None,
            quiet: bool = False,
            all_cookies: List[Dict] = None,
            proxy: str = None,
            proxy_user: Dict[str, str] = None
    ) -> None:
        if proxy_user is None:
            proxy_user = {"http_user": "http", "https_user": "https"}
        self.session: requests.Session = requests.Session()
        self.proxy: str = get_proxy(proxy)
        if self.proxy is not None:
            self.session.proxies.update({
                proxy_user.get("http_user", "http"): self.proxy,
                proxy_user.get("https_user", "https"): self.proxy
            })
        self.session.headers = HEADERS
        self.session.cookies.set("_U", auth_cookie)
        if all_cookies:
            for cookie in all_cookies:
                self.session.cookies.set(cookie["name"], cookie["value"])
        self.quiet = quiet
        self.debug_file = debug_file
        if self.debug_file:
            self.debug = partial(debug, self.debug_file)

    def get_images(self, prompt: str, timeout: int = 200, max_generate_time_sec: int = 60) -> Union[list, None]:
        """
        Fetches image links from Bing
        Parameters:
            :param prompt: str -> prompt to gen image
            :param timeout: int -> timeout
            :param max_generate_time_sec: time limit of generate image
        """
        if not self.quiet:
            print(sending_message)
        if self.debug_file:
            self.debug(sending_message)
        url_encoded_prompt = requests.utils.quote(prompt)
        payload = f"q={url_encoded_prompt}&qs=ds"
        # https://www.bing.com/images/create?q=<PROMPT>&rt=3&FORM=GENCRE
        url = f"{BING_URL}/images/create?q={url_encoded_prompt}&rt=4&FORM=GUH2CR"
        response = self.session.post(
            url,
            allow_redirects=False,
            data=payload,
            timeout=timeout,
        )
        # check for content waring message
        if "this prompt is being reviewed" in response.text.lower():
            if self.debug_file:
                self.debug(f"ERROR: {error_being_reviewed_prompt}")
            raise Exception(
                error_being_reviewed_prompt,
            )
        if "this prompt has been blocked" in response.text.lower():
            if self.debug_file:
                self.debug(f"ERROR: {error_blocked_prompt}")
            raise Exception(
                error_blocked_prompt,
            )
        if (
                "we're working hard to offer image creator in more languages"
                in response.text.lower()
        ):
            if self.debug_file:
                self.debug(f"ERROR: {error_unsupported_lang}")
            raise Exception(error_unsupported_lang)
        if response.status_code != 302:
            # if rt4 fails, try rt3
            url = f"{BING_URL}/images/create?q={url_encoded_prompt}&rt=3&FORM=GUH2CR"
            response = self.session.post(url, allow_redirects=False, timeout=timeout)
            if response.status_code != 302:
                print("Image create failed pls check cookie or old image still creating", flush=True)
                return
                # Get redirect URL
        redirect_url = response.headers["Location"].replace("&nfy=1", "")
        request_id = redirect_url.split("id=")[-1]
        self.session.get(f"{BING_URL}{redirect_url}")
        # https://www.bing.com/images/create/async/results/{ID}?q={PROMPT}
        polling_url = f"{BING_URL}/images/create/async/results/{request_id}?q={url_encoded_prompt}"
        # Poll for results
        if self.debug_file:
            self.debug("Polling and waiting for result")
        if not self.quiet:
            print("Waiting for results...")
        start_wait = time.time()
        time_sec = 0
        while True:
            if int(time.time() - start_wait) > 200:
                if self.debug_file:
                    self.debug(f"ERROR: {error_timeout}")
                raise Exception(error_timeout)
            if not self.quiet:
                print(".", end="", flush=True)
            response = self.session.get(polling_url)
            if response.status_code != 200:
                if self.debug_file:
                    self.debug(f"ERROR: {error_noresults}")
                raise Exception(error_noresults)
            if not response.text or response.text.find("errorMessage") != -1:
                time.sleep(1)
                time_sec = time_sec + 1
                if time_sec >= max_generate_time_sec:
                    raise TimeoutError("Out of generate time")
                continue
            else:
                break
        # Use regex to search for src=""
        image_links = regex.findall(r'src="([^"]+)"', response.text)
        # Remove size limit
        normal_image_links = [link.split("?w=")[0] for link in image_links]
        # Remove duplicates
        normal_image_links = list(set(normal_image_links))

        # Bad images
        bad_images = [
            "https://r.bing.com/rp/in-2zU3AJUdkgFe7ZKv19yPBHVs.png",
            "https://r.bing.com/rp/TX9QuO3WzcCJz1uaaSwQAz39Kb0.jpg",
        ]
        for img in normal_image_links:
            if img in bad_images:
                raise Exception("Bad images")
        # No images
        if not normal_image_links:
            raise Exception(error_no_images)
        return normal_image_links

    def save_images(
            self,
            links: list,
            output_dir: str,
            file_name: str = None,
            download_count: int = None,
    ) -> None:
        """
        Saves images to output directory
        Parameters:
            links: list[str]
            output_dir: str
            file_name: str
            download_count: int
        """
        if self.debug_file:
            self.debug(download_message)
        if not self.quiet:
            print(download_message)
        with contextlib.suppress(FileExistsError):
            os.mkdir(output_dir)
        try:
            fn = f"{file_name}_" if file_name else ""
            jpeg_index = 0

            if download_count:
                links = links[:download_count]

            for link in links:
                while os.path.exists(
                        os.path.join(output_dir, f"{fn}{jpeg_index}.jpeg")
                ):
                    jpeg_index += 1
                response = self.session.get(link)
                if response.status_code != 200:
                    raise Exception(f"Could not download image response code {response.status_code}")
                # save response to file
                with open(
                        os.path.join(output_dir, f"{fn}{jpeg_index}.jpeg"), "wb"
                ) as output_file:
                    output_file.write(response.content)
                jpeg_index += 1

        except requests.exceptions.MissingSchema as url_exception:
            raise Exception(
                "Inappropriate contents found in the generated images. Please try again or try another prompt.",
            ) from url_exception


class ImageGenAsync:
    """
    Image generation by Microsoft Bing
    Parameters:
        auth_cookie: str
    Optional Parameters:
        debug_file: str
        quiet: bool
        all_cookies: list[dict]
    """

    def __init__(
            self,
            auth_cookie: str = None,
            debug_file: Union[str, None] = None,
            quiet: bool = False,
            all_cookies: List[Dict] = None,
            proxy: str = None
    ) -> None:
        if auth_cookie is None and not all_cookies:
            raise Exception("No auth cookie provided")
        self.proxy: str = get_proxy(proxy)
        self.session = httpx.AsyncClient(
            proxies=self.proxy,
            headers=HEADERS,
            trust_env=True,
        )
        if auth_cookie:
            self.session.cookies.update({"_U": auth_cookie})
        if all_cookies:
            for cookie in all_cookies:
                self.session.cookies.update(
                    {cookie["name"]: cookie["value"]},
                )
        self.quiet = quiet
        self.debug_file = debug_file
        if self.debug_file:
            self.debug = partial(debug, self.debug_file)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo) -> None:
        await self.session.aclose()

    async def get_images(self, prompt: str, timeout: int = 200, max_generate_time_sec: int = 60) -> Union[list, None]:
        """
        Fetches image links from Bing
        Parameters:
            :param prompt: str -> prompt to gen image
            :param timeout: int -> timeout
            :param max_generate_time_sec: time limit of generate image
        """
        if not self.quiet:
            print("Sending request...")
        url_encoded_prompt = requests.utils.quote(prompt)
        # https://www.bing.com/images/create?q=<PROMPT>&rt=3&FORM=GENCRE
        url = f"{BING_URL}/images/create?q={url_encoded_prompt}&rt=4&FORM=GUH2CR"
        # payload = f"q={url_encoded_prompt}&qs=ds"
        response = await self.session.post(
            url,
            follow_redirects=False,
            data={"q": url_encoded_prompt, "qs": "ds"},
            timeout=timeout
        )
        content = response.text
        if "this prompt has been blocked" in content.lower():
            raise Exception(
                "Your prompt has been blocked by Bing. Try to change any bad words and try again.",
            )
        if response.status_code != 302:
            # if rt4 fails, try rt3
            url = f"{BING_URL}/images/create?q={url_encoded_prompt}&rt=3&FORM=GUH2CR"
            response = await self.session.post(
                url,
                follow_redirects=False,
                timeout=timeout,
            )
        if response.status_code != 302:
            print("Image create failed pls check cookie or old image still creating", flush=True)
            return None
        # Get redirect URL
        redirect_url = response.headers["Location"].replace("&nfy=1", "")
        request_id = redirect_url.split("id=")[-1]
        await self.session.get(f"{BING_URL}{redirect_url}")
        # https://www.bing.com/images/create/async/results/{ID}?q={PROMPT}
        polling_url = f"{BING_URL}/images/create/async/results/{request_id}?q={url_encoded_prompt}"
        # Poll for results
        if not self.quiet:
            print("Waiting for results...")
        time_sec = 0
        while True:
            if not self.quiet:
                print(".", end="", flush=True)
            # By default, timeout is 300s, change as needed
            response = await self.session.get(polling_url)
            if response.status_code != 200:
                raise Exception("Could not get results")
            content = response.text
            if content and content.find("errorMessage") == -1:
                break

            await asyncio.sleep(1)
            time_sec = time_sec + 1
            if time_sec >= max_generate_time_sec:
                raise TimeoutError("Out of generate time")
            continue
        # Use regex to search for src=""
        image_links = regex.findall(r'src="([^"]+)"', content)
        # Remove size limit
        normal_image_links = [link.split("?w=")[0] for link in image_links]
        # Remove duplicates
        normal_image_links = list(set(normal_image_links))

        # Bad images
        bad_images = [
            "https://r.bing.com/rp/in-2zU3AJUdkgFe7ZKv19yPBHVs.png",
            "https://r.bing.com/rp/TX9QuO3WzcCJz1uaaSwQAz39Kb0.jpg",
        ]
        for im in normal_image_links:
            if im in bad_images:
                raise Exception("Bad images")
        # No images
        if not normal_image_links:
            raise Exception("No images")
        return normal_image_links


async def async_image_gen(
        prompt: str,
        u_cookie=None,
        debug_file=None,
        quiet=False,
        all_cookies=None,
):
    async with ImageGenAsync(
            u_cookie,
            debug_file=debug_file,
            quiet=quiet,
            all_cookies=all_cookies,
    ) as image_generator:
        return await image_generator.get_images(prompt)


def main():
    cookie = 'MUID=2DDF291EC11B68710B7C3D39C0516929; MUIDB=2DDF291EC11B68710B7C3D39C0516929; _FP=hta=on; MMCASM=ID=7EE2FD089BCA42A1891B292470B57E15; _UR=QS=0&TQS=0&Pn=0; imgv=lodlg=2&gts=20241220; PPLState=1; SnrOvr=X=rebateson; _clck=1o5rgvq%7C2%7Cftl%7C0%7C1811; BFBUSR=BFBHP=0; _tarLang=default=en; _TTSS_IN=hist=WyJ6aC1IYW5zIiwiYXV0by1kZXRlY3QiXQ==&isADRU=0; _TTSS_OUT=hist=WyJlbiJd; SRCHD=AF=NOFORM; SRCHUID=V=2&GUID=4BE72143DAE84687955D749C95B2CA16&dmnchg=1; ANON=A=A5F634DD3D2F86B31E33B8B2FFFFFFFF&E=1e77&W=1; NAP=V=1.9&E=1e7d&C=80yvEtLmFo0dYfmtasZ6K3XPWF8bHecpeaMNiFC6EimPs0murRa1Fw&W=1; KievRPSSecAuth=FABqBRRaTOJILtFsMkpLVWSG6AN6C/svRwNmAAAEgAAACJRIBsXPiyCXKAXaq1h1fvWg90JLWpL/ddI6fY5Gh83fUJ8YkZRdK6Zy1qWeZKFfqG2KnYVPzZUUnEBWIxGxcDht05k3FU5yrGe476pQQrvSdYURqjvFBwlh44p4kdr6ZH/lLKsJfCaUei+F2Flsc68zofAM9xJNlkfJgwxEjS419ryQlveaQbVzdkDQ6MDI2QLEbdxYpu2c/T9sqSosgmIKeDBYbU91ho8K6jw33zLySEfW9J2Wg8F7NoqCcP+rcoDS49JbbKcXk8O5S8e4joEf48KUFaH9ofR/maKwbfE/OdLX7NjZFMqyYLSYLTcXHh9ym7Ul3KsiPkzi65ztRccD9rtwn7kzpFCLW90akB97rcDQmwVxJ1SwPVs/va60Uz9dyvMbpoKIo747Lvp55bEigifoLDmrjoc6ZCcKzimkAHWLYM3ki82xYokN7q7c+nYXe8JnMAes1mtYl23V98RzyiDGyQ0lGOyCiGZeeUFzgB8LGDy1Guz1vkfKNan/+2O1htyspcAu2EE1a3+5uzv4Jwjr43y68v/qeuO9ECNF5uICMspTW1ulXHvqwoSCUVBlGNgjYMVUna1yUEU4o3mOuvsjt2aiMbPeH8aqsad7f/T0yk5dKHeM36DKZTyhKrOPy/cv/x4juBQsANC42UnOoWBuQoe+Jy6h2FA4C9i6oZXRapdlkGUK4cYJ+TPs9Q8OpN5Bry23joIR2MVYZTGb1pIJRp4ZUC++duwAcaWOo+byJrCuZJgk7Q6A25XRBePvziIqbvsGqlVcRbEBfbV6TOpV9HHC01QYeEZTHF/2R6/fNvx0f9be4ufjcqqJPq5m9lQCzPxNgpLMonNpm2wn3807/rELfok9nGH8hWQ9aYnDBKNWo5w+mBemf+T1QThdyKs+HCMoE+8pn0rW5GvrzbSrj0djlT6LLpWJpNdnJjWb/Q54qrpvYsxkPySzlSwr0LyNAqh2K+iACFBBMQHwfKtZkCYXPClClmNz2Ts4l5FVfo7ZwQmBEXOlqmOvvzkb3J2f2/yMstDi2ZzxltxUvQ9nF4vnEm0tiqgs7UyfE2h2NG/3zqWFVb3iGd9YNeXWdPgnV/b1owJdC7PeM2Y+R9Pg3Lt/zkjwCTwGxE6tydjA4FCcFrdIXz3dze1ZLk8aE7Cziz9pWz50Zy1+I0fyCcNjXLKwcSRDLfaQqPGRpw6J5bg0EyYkeAm1E4JYxMlvS05kRtN4BG/fP2/eEAHaT0Yi4Rk4W1VALLP4Fh5VHIq8v53dS74EVFlliOIw2dNdu1lYRv7UzzQaGolVJJT3PKVDDJBSoi7QJQKtQyqNEo89HHrSIwRoR9QjV33BbNfJes1K6RQ3/Pbs7GrkmeJqNDg6liZeA1jHkzSFl3nJRQuQX49a2gZMfd3S+XvnKHSiGAcNpSxvVrZlJo9pOSfgc6njPGJvU/8BCNoJzwlNi5Do9HnYgANwxnueUYFuwe4bKDKD1UNBYoB6hEVYP2CZkgyFlKjclEw7Kb/ctWejR0cGp3oT4vI0N16878yP1y0G7eTnVe+R/p4ATkClO39pKm5AO/DNVRNCy8AmdBgd9wX2IJBkeivFIDZkgHuI57qCftUFcEtbM9MJYdDULwJVh2AgUCgTVfn5CKwCXQ+mt8mfjPW8n9Zc+J21IEpRvrkXy1jT5jgef+wqKXDFCuPXRxn5a7DJx686n3rKddjDXsHV1NTeVWUzNiNv6BHBDMFgCTEeKV87ZQjtDIDvtXksicSgCuZ7vSDZkkRErRiU9/AUAPEvevGT4r5yJuVJvAI5JptXE5Rm; BFPRResults=FirstPageUrls=7632B3D2D2AF080C51EA582E0FC3E138%2C1443F83E4206698296DA5B7FE90BEC64%2CA3A1B24541A00613E9DE4866CD4CF3D2%2CDA7B965F0E0AC7B0BB8592E167F8B20A%2CF2E078E304C000CD79731AE69697471C%2C90413C26AEE32CD49AD085E254B493AF%2CB461857881DC85A9B5C697B258CF9138%2CF5566CBE424D5764ABE314C6629F66C7%2CDAF2A008862F6C6426D6F3B419568004%2C9C9339D584D1F4EB0F92930BC5D9B74D&FPIG=1140CA588DDD4048913542E1FDFAC4CA; _EDGE_S=mkt=zh-cn&SID=11ADAA2A7D11632C17C1BE1A7C526286&ui=zh-cn; ENSEARCH=BENVER=0; WLS=C=&N=; _SS=SID=318E279A2E1462F9136633BD2F5E6357&R=2451&RB=2451&GB=0&RG=0&RP=2451&h5comp=3; SNRHOP=I=&TS=; USRLOC=HS=1&ELOC=LAT=39.96171951293945|LON=116.40641784667969|N=%E4%B8%9C%E5%9F%8E%E5%8C%BA%EF%BC%8C%E5%8C%97%E4%BA%AC%E5%B8%82|ELT=4|; _HPVN=CS=eyJQbiI6eyJDbiI6OCwiU3QiOjAsIlFzIjowLCJQcm9kIjoiUCJ9LCJTYyI6eyJDbiI6OCwiU3QiOjAsIlFzIjowLCJQcm9kIjoiSCJ9LCJReiI6eyJDbiI6OCwiU3QiOjAsIlFzIjowLCJQcm9kIjoiVCJ9LCJBcCI6dHJ1ZSwiTXV0ZSI6dHJ1ZSwiTGFkIjoiMjAyNS0wNC0yOFQwMDowMDowMFoiLCJJb3RkIjowLCJHd2IiOjAsIlRucyI6MCwiRGZ0IjpudWxsLCJNdnMiOjAsIkZsdCI6MCwiSW1wIjozOSwiVG9ibiI6MH0=; ipv6=hit=1745807352753&t=4; _Rwho=u=d&ts=2025-04-28; SRCHUSR=DOB=20240216&T=1743498259000&TPC=1737535886000&DS=1; _RwBf=r=1&ilt=1&ihpd=1&ispd=0&rc=2451&rb=2451&gb=2025w17_c&rg=0&pc=2451&mtu=0&rbb=0.0&g=&cid=0&clo=0&v=5&l=2025-04-27T07:00:00.0000000Z&lft=0001-01-01T00:00:00.0000000&aof=0&ard=0001-01-01T00:00:00.0000000&rwdbt=1671649281&rwflt=1683364171&rwaul2=0&o=0&p=MULTIGENREWARDSCMACQ202205&c=ML2357&t=3545&s=2022-11-25T02:50:02.2200934+00:00&ts=2025-04-28T01:29:19.5108304+00:00&rwred=0&wls=0&wlb=0&wle=1&ccp=2&cpt=0&lka=0&lkt=0&aad=0&TH=&mta=0&e=uCDLq-q6cz1TbMOrItBwUgIsjGTNwsk09vz1MgSKSFFxnQHz18y51-vQeIsSqoe6CE6VryyM7_HBqMCdgiXwfYfvJkBcHpBPPOQqw_G2ZEY&A=; _C_ETH=1; SRCHHPGUSR=SRCHLANG=zh-Hans&PV=6.0&DM=1&BRW=N&BRH=T&CW=1177&CH=1248&SCW=1177&SCH=2351&DPR=2.0&UTC=480&HV=1745803759&BZA=0&PRVCW=631&PRVCH=669&EXLTT=31&AV=14&ADV=14&RB=0&MB=0&WTS=63881400551&IG=501B66B6BC0A4B96961AEBB76E13966C&HVE=CfDJ8GtUudZcSi1Enm88WwQKtCf_bqnGJ0nLBN4aG9I8s1noj6cBxhlT4QKZwS1VcVhTKMUKkoN943Zfupe553KsH0_YSAs8zVD1MvgQvbA3XtWeFip2lQXKyx1sMThGDFEUf92acJFfGDOQg4xCD83kIz9GNajDImE-XTjMhFvDvhkuPTTVC-LORjmc4LO0Ud7cYg&DMREF=0'
    sys.argv = [sys.argv[0], '--prompt', '你是一名股票专家','-U',cookie]


    parser = argparse.ArgumentParser()
    parser.add_argument("-U", help="Auth cookie from browser", type=str)
    parser.add_argument("--cookie-file", help="File containing auth cookie", type=str)
    parser.add_argument(
        "--prompt",
        help="Prompt to generate images for",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--output-dir",
        help="Output directory",
        type=str,
        default="./output",
    )

    parser.add_argument(
        "--download-count",
        help="Number of images to download, value must be less than five",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--debug-file",
        help="Path to the file where debug information will be written.",
        type=str,
    )

    parser.add_argument(
        "--quiet",
        help="Disable pipeline messages",
        action="store_true",
    )
    parser.add_argument(
        "--asyncio",
        help="Run ImageGen using asyncio",
        action="store_true",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print the version number",
    )

    args = parser.parse_args()

    if args.version:
        sys.exit()

    # Load auth cookie
    cookie_json = None
    if args.cookie_file is not None:
        with contextlib.suppress(Exception):
            with open(args.cookie_file, encoding="utf-8") as file:
                cookie_json = json.load(file)



   #  if args.U is None and args.cookie_file is None:
   #      raise Exception("Could not find auth cookie")


    if args.download_count > 4:
        raise Exception("The number of downloads must be less than five")

    if not args.asyncio:
        # Create image generator
        image_generator = ImageGen(
            args.U,
            args.debug_file,
            args.quiet,
            all_cookies=cookie_json,
        )
        image_generator.save_images(
            image_generator.get_images(args.prompt),
            output_dir=args.output_dir,
            download_count=args.download_count,
        )
    else:
        asyncio.run(
            async_image_gen(
                args.prompt,
                args.download_count,
                args.output_dir,
                args.U,
                args.debug_file,
                args.quiet,
                all_cookies=cookie_json,
            ),
        )


if __name__ == "__main__":
    main()
