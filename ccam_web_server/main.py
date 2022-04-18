import logging
import os
import queue
import re
import sys
# import pyinotify
# import sqlite3
from bottle import redirect, request, route, run
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

logging.basicConfig(level="INFO")

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.environ.get("CCAM_WEB_SERVER_DATA_DIR", SCRIPT_DIR + "/data")
db = sqlite3.connect(DATA_DIR + "/db.sqlite3")


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
_files = []


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
    get_file_data()  # comment this for db
    page_start = 0
    page_length = 100
    showable = _files
#     _selects = []
    if re.search("page=\d+", qs):
        page_start = int(re.search("page=(\d+)", qs).group(1)) * page_length
    if re.search("year=\d+", qs):
        _year = re.search("year=(\d+)", qs).group(1)
#         _selects.append("year=%d"%int(_year))
        showable = list(set(showable).intersection(set(by_year.get(_year, []))))
    if re.search("month=\d+", qs):
        _month = re.search("month=(\d+)", qs).group(1)
#         _selects.append("month=%d"%int(_month))
        showable = list(set(showable).intersection(set(by_month.get(_month, []))))
    if re.search("day=\d+", qs):
        _day = re.search("day=(\d+)", qs).group(1)
#         _selects.append("day=%d"%int(_day))
        showable = list(set(showable).intersection(set(by_day.get(_day, []))))
    if re.search("hour=\d+", qs):
        _hour = re.search("hour=(\d+)", qs).group(1)
#         _selects.append("hour=%d"%int(_hour))
        showable = list(set(showable).intersection(set(by_hour.get(_hour, []))))
    if re.search("minute=\d+", qs):
        _minute = re.search("minute=(\d+)", qs).group(1)
#         _selects.append("minute=%d"%int(_minute))
        showable = list(set(showable).intersection(set(by_minute.get(_minute, []))))
    if re.search("cam_no=\d+", qs):
        _cam_no = re.search("cam_no=(\d+)", qs).group(1)
#         _selects.append("cam_no='%s'"%_cam_no)
        showable = list(set(showable).intersection(set(by_cam_no.get(_cam_no, []))))
#     _where = "where %s"%" AND ".join(_selects) if _selects else ""
#     _limit = page_length
#     _offset = page_start
#     _results = db.exeucte("select file from files %s limit %d offset %d"%(_where, _limit, _offset).fetchall()
    print("len of showable", len(showable))
    _tmp = (TABLE_ROW_TEMPLATE % TABLE_HEADER_CELL_TEMPLATE % "filename") + "".join(
        TABLE_ROW_TEMPLATE % TABLE_BODY_CELL_TEMPLATE % x
        for x in showable[page_start : page_start + page_length]
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

    @staticmethod
    def run_ccam_ftp_server(argv):
        # Instantiate a dummy authorizer for managing 'virtual' users
        authorizer = DummyAuthorizer()

        # Define a new user having full r/w permissions and a read-only
        # anonymous user
        authorizer.add_user(
            "pi",
            "12345",
            os.environ.get("CCAM_WEB_SERVER_DATA_DIR", os.getcwd()),
            perm="elradfmwMT",
        )
        # authorizer.add_anonymous(os.environ.get("CCAM_WEB_SERVER_DATA_DIR", os.getcwd()))

        # Instantiate FTP handler class
        handler = FTPHandler
        handler.authorizer = authorizer

        # Define a customized banner (string returned when client connects)
        handler.banner = "pyftpdlib based ftpd ready."

        # Specify a masquerade address and the range of ports to use for
        # passive connections.  Decomment in case you're behind a NAT.
        # handler.masquerade_address = '151.25.42.11'
        handler.passive_ports = range(60000, 65535)

        # Instantiate FTP server class and listen on 0.0.0.0:2121
        address = ("", 2121)
        server = FTPServer(address, handler)

        # set a limit for connections
        server.max_cons = 256
        server.max_cons_per_ip = 5

        # start ftp server
        server.serve_forever()

#     @staticmethod
#     def run_ccam_inotify(argv):
        
#         class EventHandler(pyinotify.ProcessEvent):
#             def process_IN_CREATE(self, event):
#                 try:
#                     created_at = datetime.datetime.now().isoformat()
#                     file = event.name
#                     _filename_time, cam_no = file.replace("_film.mov", "").split("_")
#                     year, month, day, hour, minute, second = _filename_time.split("-")

#                     db.execute(('insert into files (created_at, file, year, month, day, hour, minute, second, '
#  "cam_no) values ('%s', ?, ?, ?, ?, ?, ?, ?, ?)") % created_at, (file, year, month, day, hour, minute, second, cam_no))
#                     db.commit()
#                 except Exception as e:
#                     logging.exception("error in process_in_create with error %s", repr(e))
                
#         wm = pyinotify.WatchManager()  # Watch Manager
#         mask = pyinotify.IN_CREATE  # watched events
#         handler = EventHandler()
#         notifier = pyinotify.Notifier(wm, handler)
#         wdd = wm.add_watch(os.environ.get("CCAM_WEB_SERVER_DATA_DIR"), mask, rec=True)
#         notifier.loop()

#     @staticmethod
#     def run_ccam_data_cleaner(argv):
#         ...


HELP = """
stock backtesting script

1: main.py [arguments, ...]
"""


def main(argv):
    # cli entrypoint argument parser, routes to the
    # right entrypoint
    if len(argv) < 2:
        logging.error("arguments not parsable")
        logging.info(HELP)
    elif argv[1] == "run_ccam_web_server":
        EntryPoints.run_ccam_web_server(argv)
    elif argv[1] == "run_ccam_ftp_server":
        EntryPoints.run_ccam_ftp_server(argv)
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
