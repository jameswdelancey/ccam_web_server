import logging
import os
import queue
import re
import sys
from bottle import redirect, request, route, run

logging.basicConfig(level="INFO")


threads = []
thread_stop = []
tmsrp = os.environ.get("TIMESERIES_SERVER_REPO_PATH")
log_queue = queue.Queue()
if tmsrp and os.path.exists(tmsrp + "/timeseries_server/timeseries_client.py"):
    with open(tmsrp + "/timeseries_server/timeseries_client.py") as f:
        exec(f.read())
    root_logger = logging.getLogger("")
    root_logger.addHandler(logging.handlers.QueueHandler(log_queue))
    log_to_timeseries_server(threads, thread_stop, log_queue)


TEMPLATE = """\
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>get_stocks - %s</title>

  </head>
  <body>
%s
  </body>
</html>
"""
LINK_TEMPLATE = '<a href="%s">%s</a><br>'

TABLE_TEMPLATE = """\
 <table border="1">
%s
 </table> 
"""
TABLE_HEADER_CELL_TEMPLATE = """\
    <th>%s</th>
"""
TABLE_BODY_CELL_TEMPLATE = """\
    <td>%s</td>
"""
TABLE_ROW_TEMPLATE = """\
    <tr>%s</tr>    
"""
by_year = {}
by_month = {}
by_day = {}
by_hour = {}
by_minute = {}
by_cam_no = {}
_files=[]


def get_file_data():
    global by_year, by_month, by_day, by_hour, by_minute, cam_no, _files
    file_location = os.environ.get("CCAM_WEB_SERVER_DATA_DIR", "")
    _files = os.listdir(file_location)  # takes 15 sec
    _tmp = [x.replace("_film.mov", "").split("_") for x in _files]
    _tmp2 = [x[0].split("-") + [x[1]] for x in _tmp if len(x) == 2]
    by_year = {}
    by_month = {}
    by_day = {}
    by_hour = {}
    by_minute = {}
    by_cam_no = {}
    for _file, (year, month, day, hour, minute, second, cam_no) in zip(
        _files, [x for x in _tmp2 if len(x) == 7][:100]
    ):
        by_year[year] = by_year.get(year, []) + [_file]
        by_month[month] = by_month.get(month, []) + [_file]
        by_day[day] = by_day.get(day, []) + [_file]
        by_hour[hour] = by_hour.get(hour, []) + [_file]
        by_minute[minute] = by_minute.get(minute, []) + [_file]
        by_cam_no[cam_no] = by_cam_no.get(cam_no, []) + [_file]


def files(qs, title, bodylines):
    title.append("Files")
    bodylines.append(LINK_TEMPLATE % ("/", "Back"))
    bodylines.append("<h1>%s</h1>" % title[0])
    bodylines.append("query string is: %s" % repr(qs))

    # {"2022"}
    get_file_data()
    page_start = 0
    page_length = 100
    showable = _files
    if re.search("page=\d+", qs):
        page_start = int( re.search("page=(\d+)", qs).group(1)) * page_length
    if re.search("year=\d+", qs):
        _year = re.search("year=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_year.get(_year, []))))
    if re.search("month=\d+", qs):
        _month = re.search("month=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_month.get(_month, []))))
    if re.search("day=\d+", qs):
        _day = re.search("day=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_day.get(_day, []))))
    if re.search("hour=\d+", qs):
        _hour = re.search("hour=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_hour.get(_hour, []))))
    if re.search("minute=\d+", qs):
        _minute = re.search("minute=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_minute.get(_minute, []))))
    if re.search("cam_no=\d+", qs):
        _cam_no = re.search("cam_no=(\d+)", qs).group(1)
        showable = list(set(showable).intersection(set(by_cam_no.get(_cam_no, []))))
    print("len of showable", len(showable))
    _tmp = (TABLE_ROW_TEMPLATE % TABLE_HEADER_CELL_TEMPLATE % "filename") + "".join(

        TABLE_ROW_TEMPLATE % TABLE_BODY_CELL_TEMPLATE % x for x in showable[page_start:page_start+page_length]
    )

    bodylines.append(TABLE_TEMPLATE % _tmp)


ROUTE_MAP = [
    ("p=files", files),
]


def query_string_router(qs, ret):
    for match, destination in ROUTE_MAP:
        if re.search(match, qs):
            ret.append(destination)


@route("/")
def root():
    ret = []
    qs = request.query_string
    query_string_router(qs, ret)
    title = []
    bodylines = []
    if ret:
        ret[0](qs, title, bodylines)
    else:
        title.append("menu")
        bodylines.append("<h1>%s</h1>" % title[0])
        bodylines.append("query string is: %s" % repr(qs))

        links = [
            ("files", "/?p=files"),
        ]
        linkslines = [LINK_TEMPLATE % (link, text) for text, link in links]

        bodylines.extend(linkslines)
    body = "\n".join(bodylines)
    assert title, "must have added title in page function"
    return TEMPLATE % (title[0], body)


class EntryPoints:
    @staticmethod
    def run_ccam_web_server(argv):
        """cli entry point to start human interface web server"""
        logging.info("starting run_web_server")
        run(
            host="0.0.0.0",
            port=int(os.environ.get("CCAM_WEB_SERVER_UI_PORT", 8083)),
            server="paste",
        )


HELP = """
stock backtesting script

1: main.py [arguments, ...]
"""


def main(argv):
    # cli entrypoint argument parser, routes to the
    # right entrypoint
    if len(argv) < 2:
        EntryPoints.run_ccam_web_server(argv)
    else:
        logging.error("arguments not parsable")
        logging.info(HELP)


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    finally:
        thread_stop.append(None)
        log_queue.put(None)
        [t.join() for t in threads]
