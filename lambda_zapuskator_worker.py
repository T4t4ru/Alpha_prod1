import csv
import json
import logging
import os
import re
import signal
import sys
import time
import urllib
from base64 import b64decode
from io import StringIO

import xml.etree.ElementTree as ET
import boto3
from bs4 import BeautifulSoup

import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log = logger.info

END_QUEUE_NAME = "alphaprod_sku_results"

"""
Ok. Just to be clear. Look at this function. As you can see some interesting things happening there. 
We're getting results from the queue (it's a worker after all) and execute our code to get results to send them further 
to another queue (look at END_QUEUE_NAME variable). There we sending results after some magic happening inside. 
Why do we need there to be a queue? We can use several workers at the same time without any problems. 
It saves time like hell. 
"""


def lambda_handler(event, context=None):
    log(f'raw_input\t{event["body"]}')

    j = json.loads(b64decode(event["body"]).decode())
    log(f"Decoded payload: {j}")

    # Our main execution tree. I've seen there is some strange code inside, but we'll discuss it later
    result = ToolsUnited(**j).get_result()

    sqs = boto3.resource(
        "sqs",
        region_name="eu-central-1",
        aws_access_key_id=os.environ.get("ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("SECRET_ACCESS_KEY"),
    )

    queue = sqs.create_queue(
        QueueName=os.environ.get("RAW_QUEUE_NAME") or END_QUEUE_NAME
    )
    queue.send_message(MessageBody=j)

    receipt_handle = event["ReceiptHandle"]
    sqs.delete_message(ReceiptHandle=receipt_handle)

    # After a successful execution, we can return 200.
    # I'm not sure about this, and it can potentially lead to a buggy mess
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


"""
Got it from example: https://github.com/aws-samples/graceful-shutdown-with-aws-lambda/blob/main/python-demo/hello_world/app.py
Don't really know how it works but I'll leave this here just for sure.
"""


def exit_gracefully(signum, frame):
    print("[runtime] SIGTERM received")

    print("[runtime] cleaning up")
    # perform actual clean up work here.
    time.sleep(0.2)

    print("[runtime] exiting")
    sys.exit(0)

    signal.signal(signal.SIGTERM, exit_gracefully)


class NoGetterMatchesException(BaseException):
    pass


class BasicGetter:
    full_match_candidate_key = None

    # acceptable_chars = "".join([chr(i) for i in range(32, 127)])
    acceptable_chars = (
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz,."
    )

    def __init__(self, sku):
        self.sku = sku

    def clean(self, sku):
        sku = re.sub(r"\s{2,}", " ", sku).strip()
        # "".join([c for c in sku if c in self.acceptable_chars])
        cleaned = "".join(filter(lambda c: c in self.acceptable_chars, sku))
        return cleaned

    def nospaces(self, sku):
        return re.sub(r"\s", "", sku).strip()

    def get_full_match(self, candidates):
        prepared_candidates = {
            self.clean(x[self.full_match_candidate_key]): x for x in candidates
        }
        matched_item = prepared_candidates.get(self.clean(self.sku))
        if matched_item is None:
            raise NoGetterMatchesException(
                f"No matches between {len(candidates)} candidates"
            )
        return matched_item

    def process_full_item_fields(self, full_item_info, fields):
        getter_result = list()
        for k in fields:
            if str(k).startswith("$__"):
                getter_result.append(full_item_info.get(k.replace("$__", ""), ""))
            else:
                getter_result.append(k)

        log(getter_result)
        return getter_result


class SandvikGetter(BasicGetter):
    api = "https://www.sandvik.coromant.com/api/productsearch"

    headers = {
        "cookie": ".AspNetCore.Antiforgery.9fXoN5jHCXs=CfDJ8LvzeqfCWc5Fu0d4kmhGCKuld9WgRdvpAKQChl6l9Vo5vhFX4KB6HIQmv6xqyHay9DLYNCys40pLNVwT0-C7MVCDgCs1A1vjwYjJtRCQni4sp5GWbYYVpxy4CqoSSZd2nYTNwTh8woO8cI9sl7eFaTs; ARRAffinity=9711c419a659049fd2590cda84358b6b7fe4a165cdb6acbb6ef91d696e076af4; ARRAffinitySameSite=9711c419a659049fd2590cda84358b6b7fe4a165cdb6acbb6ef91d696e076af4; TS01237a27=0137808e6803c18bafcaa999a73fd5bd2c779ecd197434afc468dd123acce6cd5e3f968234861235cfe7941147ef25931a91a597d2; language=en-gb; bd0a5=1691749393755-498762782; TS01dc4fc6=01b9fba0958396bec043c9c4f251ebd1dd5d6d257b49257430df2bf23fe9060f8fed2fc9340d24f86c234bfee29feec1134d79462f; OptanonAlertBoxClosed=2023-08-15T19:04:46.755Z; OptanonConsent=isGpcEnabled=0&datestamp=2023-08-15T19%3A04%3A47.985Z&version=6.32.0&isIABGlobal=false&hosts=&consentId=88e4261f-31c5-43d7-a87c-75d0f8651e9f&interactionCount=2&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false&geolocation=NL%3BLI; country=gb; unitOfMeasurement=Metric; OClmoOot=AzYdH-SJAQAA6XuWBOhcYx6JigbBId0YyzuSVqWu_iZOCzhqye_h8FmfJn9SAVJI39mucirJwH9eCOfvosJeCA|1|1|b49a0e8c1ddd4c17e181a91a022f0282898d7f9a; bd0a03=ecRAWXbPGXqbynTgSYrzFGoXBwSVgV7AT7jbCs3av5yPOFGlR9XYR8Izuk4d6OcxvG1/NtqWKGjnki//X091PLTWPxVIySnGswIErmKwCXQz2iKGyObS+P03/RbhCECZsSMx8Ht/IXyIueGcR7Zrf8hlETfRjRcemfKZlF0Xt+AhO4Hr; XSRF-TOKEN=CfDJ8LvzeqfCWc5Fu0d4kmhGCKssZQT2pqAKAlewfqLAGMdO51dPS_bIb4O2XKJewuzmRM6lsyf1CmNuAnakOrHnSzm_m9qEUX_SQCIqGqGFI7iZ2Nlhg7y_1kTbF4m6K5iYmGLo8b5zz2ya7oddiY43MeY; ARRAffinity=a529aa0a221c4ad64f44794372b5658864efdbf3d70e7876a56adc05a1396f00; ARRAffinitySameSite=a529aa0a221c4ad64f44794372b5658864efdbf3d70e7876a56adc05a1396f00; bd0a5=1692127039907-253298493; bd0a03=lIPX+dOctSc3gjKrhZ0n4FvIWKCN+MQQHFTDqQ6Mm7H3rzO2dZS4gXVHZ9TwRAEUQmkYUqlPxq1D76SZKjlYmb6eKHc0q3TEVtdIVCWpOvbO9osMvyW8PRHehpvU4QXJtBbvNtMnpLUjLzuCT1Gies/+8ziWsnkqHGqrC/As4d1u9Uhb; TS01dc4fc6=0137808e68118c6ca12ce3aba5b9fb951d92141a19741eaee048e21a395c7a255752357edd1e644ee8300802304d9783880e29848d; TS01237a27=0137808e68118c6ca12ce3aba5b9fb951d92141a19741eaee048e21a395c7a255752357edd1e644ee8300802304d9783880e29848d"
    }

    full_match_candidate_key = "Title"

    @property
    def search_params(self):
        return {
            "query": self.sku,
            "queryContext": "CoromantGB",
            "autocompleteType": "coromantproductsearch",
        }

    def get_candidates(self):
        matches = requests.get(
            f"{self.api}/getautocompleteitems",
            headers=self.headers,
            params=self.search_params,
        )
        if matches.status_code != 200:
            return ["Access denied"]
        candidates = matches.json()
        return candidates

    def get(self):
        candidates = self.get_candidates()

        f_matches = self.get_full_match(candidates)

        full_item_info = requests.get(
            f"{self.api}/product?id={f_matches['ID']}&unitOfMeasurement=Metric&language=en-gb",
            headers=self.headers,
        ).json()

        if f_matches.get("IsObsolete"):
            replacement = "yes"
            replacement_id = full_item_info["product"]["ReplacementProduct"]
            full_item_info = requests.get(
                f"{self.api}/product?id={replacement_id}", headers=self.headers
            ).json()
        else:
            replacement = ""

        fields = [
            replacement,
            "$__ORDCODE",
            "$__MaterialID",
            "$__PRODDESCR",
            "$__EAN",
            "$__PackageQuantity",
            "$__WT",
            "$__LCS",
        ]

        getter_result = self.process_full_item_fields(full_item_info["product"], fields)

        return getter_result


class IscarGetter(BasicGetter):
    api = "https://webshop.iscar.co.uk/api"
    headers = {}
    full_match_candidate_key = "ProductName_t"

    def get(self):
        query_params = {
            "defType": "edismax",
            "q": self.sku,
            # "q.op": "AND",
            "qf": "ProductName_ten_edgengram^2 ProductName_ten_ngram sku_t_ngram ManufacturerNo_t_ngram ProductName_ten LocalField11_s BrandMfgConcat_t",
            # "fq[]": "lang_code_s:en",
            # "fl": "product_id_i,ProductName_s,group_name_s, sku,product_pic_s,ManufacturerNo_s,BrandName_s",
            # "fq[]": "-InventoryStatus_s:NLA",
            # "rawSearchStr": "HM90 ADKT 150564-PDR IC908",
            # "rows": "10",
            # "sow": "true",
        }
        request_params = urllib.parse.urlencode(
            query_params, quote_via=urllib.parse.quote_plus
        )
        r = requests.get(f"{self.api}/solr", params=request_params).json()["response"]

        full_item_info = self.get_full_match(candidates=r["docs"])

        fileds = [
            "",  # replacement
            "$__ProductName_t",  # iso
            "$__ManufacturerNo_t",  # material_id
            "$__F_SIG_17_s",  # subcl
            "$__LocalField5_s",  # EAN
            "$__PackSize_s",  # package_quantity
            "$__PackageWeight_s",  # weight
            self.get_lcs(full_item_info["product_id_i"]),  # LCS
        ]

        getter_result = self.process_full_item_fields(full_item_info, fileds)

        return getter_result

    def get_lcs(self, product_id):
        r = requests.get(
            f"https://webshop.iscar.co.uk/cat.php?action=get_stock_on_load&p={product_id}"
        )
        if r.status_code != 200:
            return "LCS not found"
        ll = r.text.splitlines()
        i, _ = next(filter(lambda x: "stock_code_container" in x[1], enumerate(ll)))

        # terrible way to deal with the HTML, but xml.ETREE fails, so meh
        lcs = int(ll[i + 1].split(">")[-2].split("<")[0])
        return lcs
        # from pyquery import PyQuery as pq
        # return pq(r.text)("div.stock_code_container span")[1].text


class TungaloyGetter(IscarGetter):
    api = "https://webshop.tungaloyuk.co.uk/api"


class TaegutecGetter(IscarGetter):
    api = "https://webshop.taegutec.co.uk/api"


class IngersollGetter(IscarGetter):
    api = "https://webshop.ingersoll-imc.de/api"


class KyoceraGetter(BasicGetter):
    api = "https://mgago40ktk-dsn.algolia.net/1/indexes/*/queries?"

    def get(self):
        params = {
            "x-algolia-application-id": "MGAGO40KTK",
            "x-algolia-api-key": "b7d9bbb6f1d63ca99bcfe717bd759c80",
        }
        a = 1

        payload = json.dumps(
            {
                "requests": [
                    {
                        "indexName": "739.filter.item",
                        # "params": f"query={self.sku}&hitsPerPage=2&page=0"
                        "params": urllib.parse.urlencode(
                            {
                                "query": self.sku,
                                "hitsPerPage": 24,
                                "page": 0,
                                "facetFilters": '["child:true","client_id:739","type:-asset",["collections:4004662142"]]',
                            },
                            quote_via=urllib.parse.quote,
                        ),
                    }
                ]
            }
        )

        response = requests.request("POST", self.api, data=payload, params=params)

        hits = response.json()["results"][0]["hits"]
        full_item_info = self.get_full_match(hits)

        material_id = full_item_info.get(
            "msc_vendor_part_number", full_item_info.get("product_id_eanedp", "")
        )

        fields = [
            "",  # replacement"
            f"{full_item_info['isopartnumber']} {full_item_info.get('insert_grade', '')}",  # material_group_grade?
            # "$__msc_vendor_part_number",  # Material ID,
            material_id,
            "$__category_as_text",  #
            self.get_EAN(material_id=material_id),  # JAN
            "$__minorderqty",
            "$__itemweight",
            "LCS unknown",
        ]

        getter_result = self.process_full_item_fields(full_item_info, fields)

        return getter_result

    def get_EAN(self, material_id):
        host = "https://www.orange-book.com"

        payload = urllib.parse.urlencode({"q": material_id, "siteId": "ob"})
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        try:
            r1 = requests.request(
                "POST", url=f"{host}/s/keyword/list", data=payload, headers=headers
            ).text
            r1 = r1.replace("undefined(", "")
            redirect = json.loads(r1[:-1])["redirectUrl"]

            r2 = requests.get(f"{host}{redirect}").text
            jan = (
                BeautifulSoup(r2)
                .find("div", class_="k-item-codes")
                .find(text=re.compile("JAN"))
                .text
            )
            jan = jan.replace("JAN：", "").strip()
            return jan
        except Exception as e:
            return "Can't retreive JAN from orange-book.com"

    def get_full_match(self, candidates):
        for c in candidates:
            # if f'{c["isopartnumber"]} {c["material_group_grade"]}' == self.clean(self.sku):
            if self.clean(
                f'{c["isopartnumber"]}{c.get("insert_grade", "")}'
            ) == self.clean(self.sku):
                return c

        raise NoGetterMatchesException(
            f"No matches between {len(candidates)} candidates"
        )


class KennametalGetter(BasicGetter):
    api = "https://www.kennametal.com/ws/v2/kmt/products/search"
    acceptable_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz."

    def get(self):
        params = {
            "query": self.sku,
            "typeAhead": "true",
            "pageSize": 8,
        }
        candidates = requests.request("GET", self.api, params=params).json()["products"]
        full_item_info = self.get_full_match(candidates)
        return full_item_info

    def get_full_match(self, candidates):
        for _ in candidates:
            candidate = self.load_candidate(_["url"])
            if self.clean(candidate[1]) == self.clean(self.sku):
                return candidate

        raise NoGetterMatchesException(
            f"No matches between {len(candidates)} candidates"
        )

    def load_candidate(self, product_path):
        r = requests.get(f"https://www.kennametal.com/us/en{product_path}")
        soup = BeautifulSoup(r.text)
        section = soup.css.select_one(".section-content .product-specifications")
        catalog_number = section.css.select_one(
            "tr.MAS_CTLG_ISO_UNPUNCT.metric td.spec-value"
        ).text
        grade = section.find(text="Grade").parent.next_sibling.next_sibling.text
        iso = f"{catalog_number} {grade}"
        material_number = section.find(
            text="SAP Material Number"
        ).parent.next_sibling.next_sibling.text
        subclass = soup.css.select_one("h3.product-name-subtitle").text
        candidate = [
            "",  # "replacement":
            iso,  # "iso":
            material_number,  # "material_number":
            subclass,  # "subclass":
            "",
            "",
            "",
            "",
        ]
        return candidate


class SecoToolsGetter(BasicGetter):
    api = "https://www.secotools.com/core/api/Products"
    full_match_candidate_key = "Designation"

    h = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    def get(self):
        url = f"{self.api}/SearchByDesignationOrItemNumber"

        query_params = {
            "SearchTerm": self.sku,
            "NaftaSearch": "false",
            "UnitType": "Metric",
        }
        request_params = urllib.parse.urlencode(
            query_params, quote_via=urllib.parse.quote_plus
        )

        candidates = requests.request(
            "POST", url, headers=self.h, data=request_params
        ).json()

        matched_item = self.get_full_match(candidates)
        full_item_info = self.load_candidate(matched_item["ItemNumber"])
        fields = [
            "",  # replacement"
            f"{full_item_info['Designation']} {full_item_info['Grade']}",  # ISO
            "$__ItemNumber",
            full_item_info["Tags"][0][-2].title(),  # category as text
            next(
                filter(lambda x: x["Name"] == "Barcode", full_item_info["Attributes"])
            )[
                "Value"
            ],  # EAN
            "$__MinSalesQty",
            next(filter(lambda x: x["Name"] == "Weight", full_item_info["Attributes"]))[
                "Value"
            ],  # Weight
            "LCS unknown",
        ]

        getter_result = self.process_full_item_fields(full_item_info, fields)
        return getter_result

    def get_full_match(self, candidates):
        for c in candidates:
            if self.clean(f"{c['Designation']} {c['Grade'] or ''}") == self.clean(
                self.sku
            ):
                return c

        raise NoGetterMatchesException(
            f"No matches between {len(candidates)} candidates"
        )

    def load_candidate(self, item_id):
        query_params = {
            "query[itemNumber]": item_id,
            "query[market]": "NL",
            "query[language]": "en-GB",
            "v": "19295",
        }
        request_params = urllib.parse.urlencode(
            query_params, quote_via=urllib.parse.quote_plus
        )
        full_item_info = requests.request(
            "GET", f"{self.api}/GetFullProduct", headers=self.h, params=request_params
        ).json()
        return full_item_info


class WalterGetter(SandvikGetter):
    api = "https://www.walter-tools.com/api/productsearch"

    headers = {
        "cookie": ".AspNetCore.Antiforgery.9fXoN5jHCXs=CfDJ8KIvHjcOeLtInrkN-VE2KR2NKHmcISuMjDI9WE41_O6BP0pEjwnug-8GLqBuQ8qsnVFZYpY7CI0rTKf3lt9U_032E-ZiRC1-YtjZLXf78XPMoTyHHe7qNZAl8wSlPo8B0lHhkFO_HtFcZXyi9hVwQjY; ARRAffinity=6ff1e39c46353b36490f8387ecbcaf4cbeab85bbaaf088ab79879167c08c408d; ARRAffinitySameSite=6ff1e39c46353b36490f8387ecbcaf4cbeab85bbaaf088ab79879167c08c408d; ASLBSA=000383d71428ecb5dd55a76efe7f3a6ba5efb66ecb7cc6486ba6292c491d36919ae1; ASLBSACORS=000383d71428ecb5dd55a76efe7f3a6ba5efb66ecb7cc6486ba6292c491d36919ae1; TS01dc4fc6=01e680a5094a4ec8042c9644cec21d506af257de84132d9f58c289c417c3eb7373d9307fe64b7d7a0a0537cc126477f1382a0e3be2; TS0132c3af=01e680a5094a4ec8042c9644cec21d506af257de84132d9f58c289c417c3eb7373d9307fe64b7d7a0a0537cc126477f1382a0e3be2; language=en-gb; brandFields=%7B%22countryCode%22%3A%22nl%22%2C%22tibp.DMTFilter%22%3A%22metric%22%2C%22tibp.TTFFilter%22%3A%22din%22%2C%22isMetricInchEditable%22%3Atrue%7D; Tibp.Wishlist.Walter=; country=nl; b4875=1693427231589-423092508; OptanonAlertBoxClosed=2023-08-30T20:27:10.662Z; XSRF-TOKEN=CfDJ8KIvHjcOeLtInrkN-VE2KR0sKox0qCLE0LwvjR0Crx-05SQqoU1r0Qbbfxo514R4ubVIEuIotIJyjdKT_s8oIjK8UUci37P3KFfWk1FxemYsZvk1pLxz4RXV3CA-e2oWJ0ZKNsBkqPqg6XxqiHSk7Vs; OptanonConsent=isGpcEnabled=0&datestamp=2023-08-30T20%3A27%3A11.070Z&version=202209.1.0&isIABGlobal=false&hosts=&consentId=74ab62b0-9dde-4e26-9b2e-1ce83ac135b9&interactionCount=2&landingPath=NotLandingPage&groups=1%3A1%2C2%3A1%2C3%3A1%2C4%3A1%2CC0015%3A1&geolocation=%3B&AwaitingReconsent=false; unitOfMeasurement=Metric; b48703=kYs3kfZBHf/fTgc0J+//iTF4eJYx6KyJpQswsTYoXZdey+Pxejvmiig5ATTAA6ABf1mKH+g3LukQ5Hw1V934oN6FFvudnbg2qW6xZKbDD3hdQpSL8wNKDtRPkKuntqrUrR92VxJPqojJHZZVqspgUu+JKjcX2TmekSdxVQiNM1oTYlKP"
    }

    @property
    def search_params(self):
        return {
            "query": self.sku.replace(",", "."),
            "queryContext": "autocompleteScopeWIC",
            "autocompleteType": "productsearch",
        }

    def get(self):
        candidates = self.get_candidates()

        matched_item = self.get_full_match(candidates)

        params = {
            "id": matched_item[self.full_match_candidate_key].lower(),
            "measurementUnit": "Metric",
            "language": "en-gb",
        }
        full_item_info = requests.get(
            f"{self.api}/getproduct", headers=self.headers, params=params
        ).json()

        fields = [
            "",  # replacement
            "$__OrderingCodeIso",
            "$__MaterialID",
            "$__CatProdDescriptionEnGb",
            "$__EanNumber",  # EAN
            "",
            "",
            "",
        ]

        getter_result = self.process_full_item_fields(
            full_item_info["items"][0], fields
        )

        return getter_result


class DormerPrametGetter(BasicGetter):
    full_match_candidate_key = "name"
    api = "https://api.cdtrfbgg54-sandvikab1-p1-public.model-t.cc.commerce.ondemand.com/occ/v2/nl/products"

    def get(self):
        candidates = self.get_candidates()

        matched_item = self.get_full_match(candidates)

        params = {
            "lang": "en",
            "fields": "code,name,summary,description,DEFAULT,classifications",
            "curr": "EUR",
        }
        full_item_info = requests.get(
            f"{self.api}/{matched_item['code']}", params=params
        ).json()
        global_vars = next(
            filter(
                lambda x: x["code"] == "DimensionsGlobal",
                full_item_info["classifications"],
            )
        )
        # id_name = {_["id"]: _['name'] for _ in global_vars["features"]}
        classifications = {
            _["id"]: _["featureValues"][0]["value"] for _ in global_vars["features"]
        }

        fields = [
            "",  # replacement
            "$__PIM_ISO0Space_STR",
            full_item_info["code"],
            full_item_info["summary"],
            "$__PLM_EAN_STR",  # EAN PLM_EAN_STR
            full_item_info["minOrderQuantity"],
            "$__PLM_GrossWeight_NUM",
            full_item_info["lifecycleStatus"],
            full_item_info["brandName"],
        ]

        getter_result = self.process_full_item_fields(classifications, fields)

        return getter_result

    def get_candidates(self):
        url = f"{self.api}/search"
        params = {
            "fields": "relatedParts(FULL)",
            "query": self.sku,
            "pageSize": "50",
            "curr": "EUR",
        }

        response = requests.request("GET", url, params=params).json()

        candidates = list()
        for cat in response["relatedParts"][0]["categories"]:
            for product in cat["products"]:
                candidates.append(product)
        return candidates


class ToolsUnited:
    reg_collection = [
        re.compile(
            r"\((.*?)\)"
        ),  # everything in parenthesis "ok (1) ok(zveg4.)" => 'ok  ok'
        re.compile(r"\s{2,}"),  # 2 or more spaces: "abc \t  vbn   oo" => 'abc vbn oo'
        re.compile(r"ART\.?\s?"),  # "Art. 48501" => "48501"
    ]

    _brands_map = None
    _sheet = None

    bc = ""
    normalized_sku = ""

    api = "https://www.toolsunited.com/App/EN"

    brand_getters = {
        "SV": SandvikGetter,
        "IS": IscarGetter,
        "TT": TungaloyGetter,
        "TAGT": TaegutecGetter,
        "IL": IngersollGetter,
        "KYO": KyoceraGetter,
        "KH": KennametalGetter,
        "SO": SecoToolsGetter,
        "W": WalterGetter,
        "DMPT": DormerPrametGetter,
    }

    def __init__(self, sku, brand=None, replacement_sku=None, operational_mode=0):
        if replacement_sku:
            self.sku = replacement_sku.upper()
            log(f"SKU-replacement: {self.sku}")
        else:
            self.sku = next(iter(sku.split(";"))).upper()

        self.brand = brand
        self.set_brand_code()

        self.operational_mode = int(operational_mode)

    @property
    def brands_map(self):
        if self._brands_map is None:
            self._brands_map = self.sheet_brands()
        return self._brands_map

    def set_brand_code(self):
        bc = self.brands_map.get(self.brand.upper())
        if bc is not None:
            self.bc = f"[{bc}] "

    def get_result(self):
        self.normalized_sku = self.normalize_sku()
        if not self.normalized_sku:
            return "?"

        if self.operational_mode == 1:
            log(f"Only returning normalized sku: {self.normalized_sku}")
            return self.normalized_sku
        elif self.operational_mode == 7:
            # it's a final match
            return self.get_final_match()
        elif self.operational_mode == 8:
            return self.get_webshop_result()
        else:
            return self.get_matches()

    @property
    def sheet(self):
        if self._sheet is None:
            s3_j = (
                boto3.resource("s3")
                .Object("aba4ff317a218482-alphaprodution", "r.json")
                .get()["Body"]
            )
            self._sheet = json.load(s3_j)
        return self._sheet

    def sheet_removals(self):
        # return json.load(boto3.resource("s3").Object("aba4ff317a218482-alphaprodution", "r.json").get()["Body"])
        return self.sheet["removals"]

    def sheet_replacements(self):
        # return json.load(boto3.resource("s3").Object("aba4ff317a218482-alphaprodution", "r.json").get()["Body"])["replacements"]
        return self.sheet["replacements"]

    def sheet_brands(self):
        return {k.upper(): v for k, v in self.sheet["brands"].items()}

    @staticmethod
    def get_sheet(gid):
        sheet_id = "1Y50D8tWLOSt1oi5vUm4SMcucjWPXJmk7LG7Mqc3drgc"
        r = requests.get(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
        )
        dr = csv.DictReader(StringIO(r.text), delimiter=",")
        return dr

    def normalize_sku(self):
        log(f"Normalizing sku: {self.sku}")

        self.normalized_sku = self.clean(self.sku)

        for removal in self.sheet_removals():
            self.normalized_sku = self.normalized_sku.replace(
                removal.upper(), ""
            ).strip()

        # replacements
        # log(replacements())
        for r_find, r_replace in self.sheet_replacements().items():
            self.normalized_sku = self.normalized_sku.replace(r_find, r_replace).strip()

        log(f"Normalized sku: {self.normalized_sku}")
        return self.normalized_sku

    def clean(self, sku):
        for rc in self.reg_collection:
            sku = re.sub(rc, " ", sku).strip()
        return sku.upper()

    def brand_name(self, brand_code):
        reversed_map = {v: k for k, v in self.brands_map.items()}
        return reversed_map.get(brand_code)

    def request_toolsunited(self):
        url = f"{self.api}/TuMenu/GetJsonResultList"

        payload = {
            "path": f'[["Volltext§{self.bc}{self.normalized_sku}"],"Root",0,100,"default",true,[],"ToolsUnited"]'
        }
        log(f"Payload: {payload}")

        jr = requests.request("POST", url, data=payload).json()
        return jr

    def get_matches(self):
        def filter_results(r):
            return self.clean(r["IDNR"]) == self.clean(self.normalized_sku)

        jr = self.request_toolsunited()["ResultList"]
        if len(jr) == 0:
            return [
                {
                    "sku": "Not found",
                    "brand": "Not found",
                }
            ]
        elif len(jr) == 1:
            return [
                {
                    "sku": jr[0]["IDNR"],
                    "brand": self.brand_name(jr[0]["CompanyCode"]),
                    "id": jr[0]["ID"],
                    **self.get_final_match(jr[0]["ID"]),
                }
            ]
        elif len(jr) > 1:
            reduced = list(filter(filter_results, jr))
            if len(reduced) == 1:
                return [
                    {
                        "sku": reduced[0]["IDNR"],
                        "brand": self.brand_name(reduced[0]["CompanyCode"]),
                        "id": reduced[0]["ID"],
                        **self.get_final_match(reduced[0]["ID"]),
                    }
                ]
            else:
                result = list()
                for i in jr:
                    result.append(
                        {
                            "sku": i["IDNR"],
                            "brand": self.brand_name(i["CompanyCode"]),
                            "id": i["ID"],
                        }
                    )
                return result

    def get_package_size(self, pos_id):
        package_size_url = f"{self.api}/Article/GetOrderTableComponent?id={pos_id}&dataSource=toolsunited"
        package_size_t = requests.get(package_size_url).text
        tdtd = ET.ElementTree(ET.fromstring(package_size_t)).findall(".//td")
        for i, td in enumerate(tdtd):
            if "Package unit" in td.text:
                return tdtd[i + 1].text

        return ""

    def get_final_match(self, tu_record_id):
        url = f"{self.api}/Article/GetJsonArticleDetails?article={tu_record_id}&dataSource=toolsunited"
        first_full_result = requests.get(url).json()
        rsv_map = {v["NormDescription"]: v for v in first_full_result["DetailFields"]}
        j20_value = rsv_map["J20"]["Value"]
        j3_value = rsv_map["J3"]["Value"]
        d75_value = rsv_map.get("D75", {}).get("Value", "")
        package_size = self.get_package_size(tu_record_id)

        return dict(
            j3_value=j3_value,
            j20_value=j20_value,
            d75_value=d75_value,
            package_size=package_size,
        )

    def get_webshop_result(self):
        getter_class = self.brand_getters.get(self.brands_map.get(self.brand.upper()))
        if getter_class is None:
            return [["Invalid brand"]]
        else:
            try:
                getter_result = getter_class(self.normalized_sku).get()
                return [[*getter_result]]
            except NoGetterMatchesException as e:
                return [[str(e), "", "", "", "", "", "", ""]]
