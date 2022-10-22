from bs4 import BeautifulSoup
import logging
import os
import re
import requests
import sys

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger('')
root_domain = "http://discmaster.textfiles.com"
webs_only = False
web_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0"
}

def to_absolute_url(url):
    if url.startswith("/"):
        url = root_domain + url
    # especial 4
    url = re.sub("NÂº", "Nº", url)
    return url

def get_text(url):
    url = to_absolute_url(url)
    cache_file = "cache/%s" % re.sub(r'[^0-9a-zA-Z]', '_', url)
    data = None
    if os.path.isfile(cache_file):
        logger.debug(f"using cache for {url}")
        with open(cache_file, "r") as f:
            data = f.read()
    else:
        data = requests.get(url, headers=web_headers).text
        with open(cache_file, "w") as f:
            f.write(data)
    return data

def get_html(url):
    return BeautifulSoup(get_text(url), 'html.parser')

def print_ini_file(prefix_url, dir0_name, category_offset=5):
    if "gres" in dir0_name.lower():
        return
    if webs_only and dir0_name.lower() != "webs":
        return
    ini_url = prefix_url.replace("/view/", "/file/")
    # ini_file = bytes(get_text(ini_url), 'iso-8859-3').decode('utf-8')
    ini_file = get_text(ini_url)
    dir0_category_wrap = "=" * category_offset
    print(f"{dir0_category_wrap} {dir0_name} {dir0_category_wrap}\n")
    if "[" in ini_file:
        # new format
        last_category_name = None
        for line in ini_file.splitlines():
            line = line.strip()
            if len(line) <= 0:
                continue
            if "hentai" in line.lower():
                continue
            if line.startswith(";"):
                continue # INI comment
            if line.startswith("["):
                last_category_name = re.sub("\\\\", " -> ", line[1:-1])
            elif ("=" in line) and (last_category_name is not None):
                line_data = line.split("=", maxsplit=1)
                line_key = line_data[0].strip()
                line_value = line_data[1].strip()
                line_value = re.sub("\\\\", "/", line_value)
                line_verb = "/view"
                if ("*." in line_value):
                    line_verb = "/browse"
                    line_value = line_value[0:line_value.index("*.")]
                elif ("." not in line_value):
                    line_verb = "/browse"
                line_value = requests.compat.urljoin(prefix_url, line_value)
                line_value = to_absolute_url(line_value)
                line_value = re.sub(" ", "%20", line_value)
                if line_value.endswith("/"):
                    line_value = line_value[:-1]
                line_value = re.sub("com/view", "com" + line_verb, line_value)
                if line_value.lower().endswith(".ini"):
                    print_ini_file(line_value, last_category_name)
                else:
                    print(f"  * [[{line_value}|{last_category_name}]]")
            # if line.startswith("["):
            #    categories = list(filter(lambda x: len(x) > 0, line[1:-1].split("\\")))
            #    category_level = max(1, 5 - len(categories))
            #    category_wrap = "=" * category_level_current
            #    print(f"{category_wrap} {categories[-1]} {category_wrap}\n")
            # TODO
    else:
        category_level_current = category_offset
        category_level_map = {}
        for line in ini_file.splitlines():
            if "hentai" in line.lower():
                continue
            line_st = line.lstrip()
            if line_st.startswith(";"):
                continue # INI comment
            line_offset = len(line) - len(line_st)
            if line_offset in category_level_map:
                category_level_current = category_level_map[line_offset]
            else:
                if category_level_current > 1:
                    category_level_current -= 1
                category_level_map[line_offset] = category_level_current
            if line_st.startswith("+"):
                line_st = line_st[1:]
            if "=" in line_st:
                line_data = line_st.split("=", maxsplit=1)
                line_key = line_data[0].strip()
                line_value = line_data[1].strip()
                line_value = re.sub("\\\\", "/", line_value)
                line_verb = "/view"
                if "." not in line_value:
                    line_verb = "/browse"
                line_value = requests.compat.urljoin(prefix_url, line_value)
                line_value = to_absolute_url(line_value)
                line_value = re.sub(" ", "%20", line_value)
                if line_value.endswith("/"):
                    line_value = line_value[:-1]
                line_value = re.sub("com/view", "com" + line_verb, line_value)
                print(f"  * [[{line_value}|{line_key}]]")
            elif len(line_st) > 0:
                category_wrap = "=" * category_level_current
                print(f"{category_wrap} {line_st} {category_wrap}\n")

index = get_html("http://discmaster.textfiles.com/cd-rom/")

for el in index.select("a"):
    text_u = el.get_text(strip=True)
    text = text_u.lower()
    if (text.startswith("dokan") and len(text) >= 7) or (text.startswith("animedia") and len(text) >= 10):
        print(f"====== {text_u} ======\n")
        arch_list_href = el.attrs["href"]
        arch_list = get_html(arch_list_href)
        for el in arch_list.select("a"):
            if ("href" in el.attrs) and (arch_list_href in el.attrs["href"]):
                root_file_list_href = el.attrs["href"]
                root_file_list = get_html(root_file_list_href)
                dir_tables = root_file_list.select("table")
                if len(dir_tables) < 1:
                    continue
                dir_table = dir_tables[0]

                dir_first_a = dir_table.select("a")
                if len(dir_first_a) > 0:
                    dir_first_a = dir_first_a[0]
                    if ("href" in dir_first_a.attrs) and dir_first_a.get_text(strip=True).lower().endswith(".mdf"):
                        root_file_list_href = dir_first_a.attrs["href"]
                        root_file_list = get_html(root_file_list_href)
                        dir_tables = root_file_list.select("table")
                        if len(dir_tables) < 1:
                            continue
                        dir_table = dir_tables[0]

                print(f"[[{to_absolute_url(root_file_list_href)}|Browse files]] (if the automated URLs don't work)\n")

                for el in dir_table.select("a"):
                    if ("href" in el.attrs) and (arch_list_href in el.attrs["href"]):
                        dir0_name = el.get_text(strip=True)
                        dir0_file_list_href = el.attrs["href"]
                        dir0_file_list = get_html(dir0_file_list_href)
                        for tbl in dir0_file_list.select("table"):
                            for el in tbl.select("a"):
                                el_fn_lower = el.get_text(strip=True).lower()
                                if ("href" in el.attrs) and (el_fn_lower.endswith(".ini")):
                                    # found ini file
                                    if el_fn_lower == "ie4setup.ini":
                                        continue
                                    if el_fn_lower == "ie5setup.ini":
                                        continue
                                    if el_fn_lower == "iesetup.ini":
                                        continue
                                    prefix_url = el.attrs["href"]
                                    print_ini_file(prefix_url, dir0_name)
