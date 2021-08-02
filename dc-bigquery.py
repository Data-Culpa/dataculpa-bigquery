#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dc-bigquery.py
# Data Culpa Google BigQuery Connector
#
# Copyright (c) 2021 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#

import argparse
import json
import logging
import os
import pickle
import sqlite3
import sys
import time
import traceback
import yaml

import dotenv

from dateutil import parser
from datetime import datetime, timedelta, timezone

from dataculpa import DataCulpaValidator
from google.cloud import bigquery
from google.cloud.bigquery import dbapi

DC_DEBUG = os.environ.get('DC_DEBUG', False)

if False:
    for k,v in  logging.Logger.manager.loggerDict.items():
        if k.find(".") > 0:
            continue
        print(k)#, v)
        print("---")

for logger_name in ['urllib3', 'botocore', 'boto3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARN)

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger('dataculpa')

def FatalError(rc, message):
    sys.stderr.write(message)
    sys.stderr.write("\n")
    sys.stderr.flush()
    sys.exit(rc)
    return

class Config:
    def __init__(self):
        self._d = {
                    'dataculpa_controller': {
                        'host': 'localhost',
                        'port': 7777,
                    },
                    'configuration': {
                        'session_history_cache': 'session_history_cache.db',
                        'table_list': [
                            { 'table': 'example-test', 
                              'desc_order_by': 'column-to-sort-by',
                              'initial_limit': 50000,
                              'dc-nickname': 'my-table',
                              'timeshift': 'true to interpret the desc_order_by column as a date and push that metadata to Validator'
                            }
                        ]
                    },
                    'dataculpa_pipeline': {
                        'name': 'bq-$TABLE',
                    }
        }


    def save(self, fname):
        if os.path.exists(fname):
            logger.error("%s exists already; rename it before creating a new example config." % fname)
            sys.exit(1)
            return

        f = open(fname, 'w')
        yaml.safe_dump(self._d, f, default_flow_style=False)
        f.close()
        return

    def load(self, fname):
        with open(fname, "r") as f:
            #print(f)
            self._d = yaml.load(f, yaml.SafeLoader)
            #print(self._d)

        # No--just keep this in main.
        #dotenv.load_dotenv(env_file)

        # FIXME: error checking?
        # dump to stderror, non-zero exit... maybe need an error path to push to Data Culpa.
        return

    def load_env(self, env_file):
        return

    def get_config(self):
        return self._d.get('configuration')

    def get_cache_file(self):
        return self.get_config().get('session_history_cache', 'session_history_cache.db')

    def get_table_list(self):
        return self.get_config().get('table_list')

    def get_controller(self):
        return self._d.get('dataculpa_controller')

    def get_pipeline(self):
        return self._d.get('dataculpa_pipeline')

    def get_pipeline_name(self):
        return self.get_pipeline().get('name')

    def connect_controller(self, table_name, timeshift=0):
        pipeline_name = self.get_pipeline_name()

        if pipeline_name.find("$TABLE") >= 0:
            pipeline_name = pipeline_name.replace("$TABLE", table_name)

        cc = self.get_controller()
        host = cc.get('host')
        port = cc.get('port')

        # FIXME: load Data Culpa secret
        v = DataCulpaValidator(pipeline_name,
                               protocol=DataCulpaValidator.HTTP,
                               dc_host=host,
                               dc_port=port,
                               timeshift=timeshift,
                               queue_window=1000)
        return v


class SessionHistory:
    def __init__(self):
        self.history = {}
        self.config = None
        self.write_enabled = True

    def set_config(self, config):
        assert isinstance(config, Config)
        self.config = config

    def set_write_enabled(self, yesno):
        self.write_enabled = yesno
        return

    def add_history(self, table_name, field, value):
        assert self.config is not None
        self.history[table_name] = (field, value)
        return

    def has_history(self, table_name):
        return self.history.get(table_name) is not None

    def get_history(self, table_name):
        return self.history.get(table_name)

    def _get_existing_tables(self, cache_path):
        assert self.config is not None
        _tables = []
        c = sqlite3.connect(cache_path)
        r = c.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'")
        for row in r:
            _tables.append(row[0])
        return _tables

    def _handle_new_cache(self, cache_path):
        assert self.config is not None
        _tables = self._get_existing_tables(cache_path)

        c = sqlite3.connect(cache_path)
        if "cache" not in _tables:
            c.execute("create table cache (object_name text unique, field_name text, field_value)")

        if "sql_log" not in _tables:
            c.execute("create table sql_log (sql text, object_name text, Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")

        c.commit()

        # endif

        return

    def append_sql_log(self, table_name, sql_stmt):
        assert self.config is not None
        assert isinstance(table_name, str)
        assert isinstance(sql_stmt, str)

        cache_path = self.config.get_cache_file()
        c = sqlite3.connect(cache_path)
        self._handle_new_cache(cache_path)
        c.execute("insert into sql_log (sql, object_name) values (?,?)", (sql_stmt, table_name))
        c.commit()
        return

    def save(self):
        assert self.config is not None

        if not self.write_enabled:
            logger.warning("write_enabled is TURNED OFF for cache")
            return
        # endif

        # write to disk
        cache_path = self.config.get_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        for table, f_pair in self.history.items():
            (fn, fv) = f_pair
            fv_pickle = pickle.dumps(fv)
            # Note that this might be dangerous if we add new fields later and we don't set them all...
            #print(table, fn, fv)
            c.execute("insert or replace into cache (object_name, field_name, field_value) values (?,?,?)",
                      (table, fn, fv_pickle))

        c.commit()

        return

    def load(self):
        assert self.config is not None

        # read from disk
        cache_path = self.config.get_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        r = c.execute("select object_name, field_name, field_value from cache")
        for row in r:
            (table, fn, fv_pickle) = row
            fv = pickle.loads(fv_pickle)
            self.add_history(table, fn, fv)
        # endfor
        return

gCache = SessionHistory()

def getMeta(bq_client, table, t_order_by):
    # build up min/maxes in case it's useful for debugging.
    # FIXME: if we have to translate types for this later, this might not be particularly useful.
    meta = {}
    
    if t_order_by is not None:
        global_min_sql = "select min(%s) from %s"  % (t_order_by, table)
        global_max_sql = "select max(%s) from %s"  % (t_order_by, table)
        global_count   = "select count(*) from %s" % (table,)

        gCache.append_sql_log(table, global_min_sql)
        job = bq_client.query(global_min_sql)
        for row in job:
            min_r = row[0]

        gCache.append_sql_log(table, global_max_sql)
        job = bq_client.query(global_max_sql)
        for row in job:
            max_r = row[0]

        gCache.append_sql_log(table, global_count)
        job = bq_client.query(global_count)
        for row in job:
            count_r = row[0]
        
        meta['min_%s' % table]   = min_r
        meta['max_%s' % table]   = max_r
        meta['count_%s' % table] = count_r
    # endif

    return meta

def buildSqlSuffix(table, t_use_timeshift, t_order_by, t_initial_limit):
    gCache.load()
    
    ORDER_BY = "order_by"
    LIMIT = "limit"
    WHERE = "where"

    parts = {}

    if t_order_by is not None:
        parts[ORDER_BY] = "ORDER BY %s DESC" % t_order_by

    do_limit = False
    if t_initial_limit is not None:
        if t_use_timeshift:
            # When we use timeshift, only limit the initial load when we have no cache market
            if not gCache.has_history(table):
                do_limit = True
        else:
            do_limit = True
        # endif
    # endif

    if do_limit:
        parts[LIMIT] = "LIMIT %s" % t_initial_limit

    # check our history.
    if not t_use_timeshift:
        logger.info("Timeshift is disabled; loading everything")

    if t_use_timeshift:
        marker_pair = gCache.get_history(table)
        if marker_pair is not None:
            (fk, fv) = marker_pair
            parts[WHERE] = "WHERE %s > '%s'" % (fk, fv)
        # else... hopefully it's a new load.
        # endif
    # endif
    
    if DC_DEBUG:
        logger.warning("DC_DEBUG is set")
        if not do_limit:
            parts[LIMIT] = "LIMIT 100"
            do_limit = True
    # endif

    suffix = "%s %s %s" % (parts.get(WHERE, ""), parts.get(ORDER_BY, ""), parts.get(LIMIT))
    return suffix

def FetchTable(table, t_nickname, config, t_order_by, t_initial_limit, t_use_timeshift):
    logger.info("fetching ... %s (aka %s)", table, t_nickname)

    did_log_debug = False

    bq_client = bigquery.Client()

    field_types = {}
    field_names = []

    # build select.
    # ok we need to see if we have fetched this table before..

    meta = getMeta(bq_client, table, t_order_by)
    logger.info("META = %s", meta)

    sql_prefix = "select * from %s " % (table,)
    sql_suffix = buildSqlSuffix(table, t_use_timeshift, t_order_by, t_initial_limit)
    
    sql = sql_prefix + sql_suffix
    
    logger.info("Running __%s__" % sql)

    ts = time.time()

    logger.debug(sql)
    gCache.append_sql_log(table, sql)
    gCache.save()

    job = bq_client.query(sql)

    dt = time.time() - ts

    meta['sql_query'] = sql
    meta['sql_processing_time'] = dt        # potentially useless.

    cache_marker = None

    total_r_count = 0

    if not t_use_timeshift:
        dc = config.connect_controller(t_nickname, timeshift=None)
        dc.queue_metadata(meta)

        for row in job:
            total_r_count += 1
            df_entry = {}

            if len(field_names) == 0:
                field_names = list(row.keys())
                # assuming BQ provides a uniform table?
            # endif
            
            for i in range(0, len(row)):
                df_entry[field_names[i]] = row[i]

            dc.queue_record(df_entry)
        # endfor
        
        (_queue_id, _result) = dc.queue_commit()
        if _result.get('had_error', True):
            logger.warning("Error: %s", _result)
        
        if True: #DC_DEBUG:
            logger.info("total_r_count = %s", total_r_count)
        return
    # endif

    # 
    # Timeshift operation
    #
    timeshift_r_count = 0

    # we want to set a timeshift.
    last_timeshift = 0
    dc = None # Delay opening the connection til we are ready.

    for row in job:
        total_r_count += 1
        timeshift_r_count += 1
        df_entry = {}
        this_timeshift = None

        if len(field_names) == 0:
            field_names = list(row.keys())
            # assuming BQ provides a uniform table?
        # endif

        for i in range(0, len(row)):
            df_entry[field_names[i]] = row[i]

            if field_names[i] == t_order_by:
                this_timeshift = row[i]
            # endif

        if this_timeshift is not None:
            if isinstance(this_timeshift, str):
                try:
                    this_timeshift = parser.parse(this_timeshift)
                except:
                    logger.exception("Error parsing __%s__" % this_timeshift)

            dt_now = datetime.now(timezone.utc)
            try:
                dt_delta = dt_now - this_timeshift
            except:
                dt_delta = datetime.now() - this_timeshift # no timezone.

            dt_delta_ts = dt_delta.total_seconds()
            if (abs(dt_delta_ts - last_timeshift) > 86400):
                last_timeshift = dt_delta_ts
                #print("this_timeshift = ", dt_delta_ts)

                meta['record_count'] = timeshift_r_count
                timeshift_r_count = 0
                if dc is not None:
                    dc.queue_metadata(meta)
                    (_queue_id, _result) = dc.queue_commit()
                    if _result.get('had_error', True):
                        logger.warning("Error: %s", _result)
                # endif

                dc = config.connect_controller(t_nickname, timeshift=last_timeshift)
            # endif
        # endif

        if dc is None:
            dc = config.connect_controller(t_nickname, timeshift=0)
        
        dc.queue_record(df_entry)
        cache_marker = this_timeshift

        # just for debugging
        if DC_DEBUG:
            if total_r_count > 100:
                if not did_log_debug:
                    logger.warning("DC_DEBUG is set; stopping at 100 rows")
                    did_log_debug = True
                break

    if total_r_count > 0:
        if cache_marker is None:
            if t_order_by is not None:
                logger.error("ERROR: we specified an ORDER BY constraint __%s__ for caching that is missing from the table schema: __%s__" % (t_order_by, field_names))
                sys.exit(2)
        else:
            # OK, save it off...
            gCache.add_history(table, t_order_by, cache_marker)
            gCache.save()
        # endif
    # endif

    if DC_DEBUG:
        logger.info("total_r_count = %s", total_r_count)

    meta['record_count'] = timeshift_r_count
    if dc is not None:
        dc.queue_metadata(meta)
        (_queue_id, _result) = dc.queue_commit()
        if _result.get('had_error', True):
            logger.warning("Error: %s", _result)
    else:
        if total_r_count != 0:
            logger.error("Never setup a connection to DC; total record count = %s", total_r_count)
    # FIXME: On error, rollback the cache
    return

def do_init(filename):
    print("Initialize new file")
    config = Config()
    config.save(filename)

    # Put out an .env template too.
    #with open(filename + ".env", "w") as f:
    #    #f.write("DC_CONTROLLER_SECRET=empty\n")
    #    f.close()

    return

def do_test(filename):
    print("test with config from file %s" % filename)

    bq_client = bigquery.Client() 

    config = Config()
    config.load(filename)
    gCache.set_config(config)

    # get the table list...
    table_list = config.get_table_list()
    if not table_list:
        FatalError(1, "no tables listed in config file to triage!")
        return

    for t in table_list:
        t_name = t.get('table')

        # get one item from the table.
        query = "SELECT * FROM %s LIMIT 1" % (t_name,)
        job = bq_client.query(query)
        for row in job:
            print("---> %s: columns: %s" % (t_name, ", ".join(list(row.keys()))))

    return

def do_run(filename, table_name=None, nocache_mode=False):
    logger.info("run with config from file %s" % filename)
    config = Config()
    config.load(filename)
    gCache.set_config(config)
    gCache.set_write_enabled(not nocache_mode)

    # get the table list...
    table_list = config.get_table_list()
    if not table_list:
        FatalError(1, "no tables listed to triage!")
        return

    for t in table_list:
        t_name          = t.get('table')
        t_order_by      = t.get('desc_order_by')
        t_initial_limit = t.get('initial_limit')
        t_use_timeshift = t.get('timeshift', False) # True/False
        t_nickname      = t.get('dc-nickname', t_name)

        if t_use_timeshift != True:
            t_use_timeshift = False
            logger.info("Weird format for timeshift found; setting to %s", t_use_timeshift)
        # endif

        # a little safeguard while we test
        if t_initial_limit is None:
            t_initial_limit = 1000
            logger.info("No initial_limit found; setting to %s", t_initial_limit)
        # endif

        if table_name is not None:
            if t_name.lower() == table_name.lower():
                FetchTable(t_name, t_nickname, config, t_order_by, t_initial_limit, t_use_timeshift)
        else:
            # normal operation
            FetchTable(t_name, t_nickname, config, t_order_by, t_initial_limit, t_use_timeshift)
        # endif
    # endfor

    return

def main():
    ap = argparse.ArgumentParser()
    #ap.add_argument("-e", "--env",
    #                help="Use provided env file instead of default .env")

    ap.add_argument("--init", help="Init a yaml config file to fill in.")
    ap.add_argument("--test", help="Test the configuration specified.")
    ap.add_argument("--run",  help="Normal operation: run the pipeline")

    args = ap.parse_args()

    if args.init:
        do_init(args.init)
        return
    else:
        #env_path = ".env"
        #if args.env:
        #    env_path = args.env
        #if not os.path.exists(env_path):
        #    sys.stderr.write("Error: missing env file at %s\n" % os.path.realpath(env_path))
        #    sys.exit(1)
        #    return
        # endif

        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
            sys.stderr.write("Error: missing GOOGLE_APPLICATION_CREDENTIALS in environment\n")
            sys.exit(2)
            return

        elif args.test:
            #dotenv.load_dotenv(env_path)
            do_test(args.test)
            return
        elif args.run:
            #dotenv.load_dotenv(env_path)
            do_run(args.run)
            return
        # endif
    # endif

    ap.print_help()
    return

if __name__ == "__main__":
    main()
