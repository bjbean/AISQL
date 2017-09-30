#!/usr/bin/python
import re

NUM_REG = '\s*\d+.?\d*\s*'
STR_REG = '.*'

SLOWLOG_STRU = {
    # Time: 2016-09-01T08:11:21.428241Z
    # User@Host: root[root] @ localhost []  Id:     2
    # Query_time: 1.014384  Lock_time: 0.001060 Rows_sent: 0  Rows_examined: 0
    #SET timestamp=1472717481;
    #ALTER USER 'root'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    'time': ['^# Time: ', '$'],
    'user': ['^# User@Host:', 'Id:'],
    'id': ['^^# User@Host:' + STR_REG + 'Id:', '$'],
    'query_time': ['^# Query_time:', ' Lock_time:'],
    'lock_time': ['^# Query_time:' + NUM_REG + ' Lock_time:', 'Rows_sent:'],
    'rows_sent': ['^# Query_time:' + NUM_REG + ' Lock_time:' + NUM_REG + 'Rows_sent:', 'Rows_examined:'],
    'rows_examined': ['^# Query_time:' + NUM_REG + ' Lock_time:' + NUM_REG + 'Rows_sent:' + NUM_REG + 'Rows_examined:',
                      '$']
}


def deal_log(v_logfile):
    result_dict = {}
    sql_num = 0
    sql_text = ''

    # read file
    f = open(v_logfile, "r")
    lines = f.readlines()

    # parse log data
    regex_section = re.compile("^# Time:")

    for line in lines:
        if regex_section.match(line):
            if sql_num > 0:
                result_dict['sql' + str(sql_num)]['sql_text'] = sql_text
                sql_text = ''

            sql_num += 1
        if sql_num == 0:
            continue

        if re.findall('^#.*', line):
            for item in SLOWLOG_STRU.keys():
                re_str = SLOWLOG_STRU[item][0] + '' + STR_REG + '' + SLOWLOG_STRU[item][1]
                print re_str
                print line
                if re.findall(re_str, line):
                    if SLOWLOG_STRU[item][1] == "$":
                        val = re.split(SLOWLOG_STRU[item][0], line)[-1]
                    else:
                        val = re.split(SLOWLOG_STRU[item][1], re.split(SLOWLOG_STRU[item][0], line)[-1])[0]

                    if result_dict.has_key('sql' + str(sql_num)) == 0:
                        result_dict['sql' + str(sql_num)] = {}
                    result_dict['sql' + str(sql_num)][item] = val.strip()
                    # result_dict['sql' + str(sql_num)].append({item: val.strip()})
        else:
            sql_text = sql_text + line
    return result_dict


if __name__ == "__main__":
    width = 15
    parse_result = deal_log('D:\hanfeng\dev\python\slow_query_log\slow.log')
    # pprint.pprint(parse_result)

    # sqlid,time,user,id,query_time,lock_time,rows_sent,rows_examined,sql_text
    for key in "sqlid,time,query_time,lock_time,rows_sent,rows_examined".split(','):
        print "\033[1;31;40m%s\033[0m" % key[:width].rjust(width),
    print

    num = 0
    for key_data in sorted(parse_result.keys()):
        num += 1
        if num > 10:
            for key in "sqlid,time,query_time,lock_time,rows_sent,rows_examined".split(','):
                print "\033[1;31;40m%s\033[0m" % key[:width].rjust(width),
            print
            num = 0
        print key_data[:width].rjust(width),
        for key_column in "time,query_time,lock_time,rows_sent,rows_examined".split(','):
            print parse_result[key_data][key_column][:(width)].rjust(width),
        print
