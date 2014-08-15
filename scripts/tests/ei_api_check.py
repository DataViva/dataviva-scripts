import requests
import json
from api_check import check_urls

if __name__ == "__main__":
    parameters = {"year": 2013, "bra1":'mg030000', "bra2":'mg030000', "cnae1": "43991",
                  "cnae2": "01113", "hs_id": "010101", "hs_id2": "052508", "hs_id3":"020713" }

    endpoints = [
        "/ei/<year>/<bra1>/<cnae1>/all/all/show",
        "/ei/<year>/all/all/<bra2>/<cnae2>/show",
        "/ei/<year>/<bra1>/all/<bra2>/all/show",
        "/ei/<year>/all/<cnae1>/all/<cnae2>/show",
        "/ei/<year>/all/<cnae1>/<bra2>/all/show",
        "/ei/<year>/<bra1>/all/all/<cnae2>/show",
        "/ei/<year>/<bra1>/<cnae1>/<bra2>/all/show",
        "/ei/<year>/<bra1>/all/<bra2>/<cnae2>/show",
        "/ei/<year>/<bra1>/<cnae1>/all/<cnae2>/show",
        "/ei/<year>/<bra1>/all/<bra2>/<cnae2>/show",
        "/ei/<year>/<bra1>/<cnae1>/<bra2>/<cnae2>/show",
        "/ei/<year>/<bra1>/all/all/show/all",
        "/ei/<year>/all/all/<bra2>/show/all",
        "/ei/<year>/all/<cnae1>/all/show/all",
        "/ei/<year>/all/all/all/show/<hs_id>",
        "/ei/<year>/<bra1>/<cnae1>/all/show/all",
        "/ei/<year>/all/all/<bra2>/show/<hs_id>",
        "/ei/<year>/<bra1>/all/<bra2>/show/all",
        "/ei/<year>/all/<cnae1>/all/show/<hs_id>",
        "/ei/<year>/all/<cnae1>/<bra2>/show/all",
        "/ei/<year>/<bra1>/all/all/show/<hs_id>",
        "/ei/<year>/<bra1>/<cnae1>/<bra2>/show/all",
        "/ei/<year>/<bra1>/all/<bra2>/show/<hs_id>",
        "/ei/<year>/<bra1>/<cnae1>/all/show/020713",
        "/ei/<year>/<bra1>/all/<bra2>/show/020713",
        "/ei/<year>/<bra1>/<cnae1>/<bra2>/show/<hs_id2>",
        "/ei/<year>/all/all/show/<cnae2>/all",
        "/ei/<year>/all/<cnae1>/show/all/all",
        "/ei/<year>/all/all/show/all/<hs_id>",
        "/ei/<year>/<bra1>/<cnae1>/show/all/all",
        "/ei/<year>/all/all/show/<cnae2>/<hs_id>",
        "/ei/<year>/<bra1>/all/show/<cnae2>/all",
        "/ei/<year>/all/<cnae1>/show/all/<hs_id>",
        "/ei/<year>/all/<cnae1>/show/<cnae2>/all",
        "/ei/<year>/<bra1>/all/show/all/<hs_id>",
        "/ei/<year>/<bra1>/<cnae1>/show/<cnae2>/all",
        "/ei/<year>/<bra1>/all/show/<cnae2>/<hs_id3>",
        "/ei/<year>/<bra1>/<cnae1>/show/all/<hs_id3>",
        "/ei/<year>/<bra1>/all/show/<cnae2>/<hs_id3>",
        "/ei/<year>/<bra1>/<cnae1>/show/<cnae2>/157307",
    ]


    check_urls(endpoints, parameters)
