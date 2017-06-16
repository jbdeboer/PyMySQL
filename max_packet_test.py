import pymysql.cursors
import mysql.connector

max_allowed_packet = 16 * 1024 * 1024  # Or fetch from DB
print('Max allowed packet size: %s bytes' % max_allowed_packet)
print('pymysql version: ', pymysql.VERSION)

def cursor():
    m = pymysql.connect(
        host='ebb.huronbox.com',
        user='pra',
        password='XXXX',
        db='pra',
        autocommit=True,
        # charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    return m.cursor()


def use_pymsql():

    with cursor() as cc:
        try:

            cc.execute(
                'CREATE TABLE IF NOT EXISTS test_max_packet (col LONGTEXT)')

            # Create a string longer than max_allowed_packet.
            longstr = ''.join(['x' for x in range(max_allowed_packet * 2)])
            print('Long string is %s bytes' % len(longstr))
            cc.execute('INSERT INTO test_max_packet (col) VALUES (%s)',
                       longstr)

            cc.execute('SELECT count(*) FROM test_max_packet')
            print(cc.fetchone())
        finally:

            with cursor() as cc:
                cc.execute('DROP TABLE test_max_packet')


def use_mysql_connector():
    m = mysql.connector.connect(host='ebb.huronbox.com',
                                user='pra',
                                password='prapassword',
                                database='pra',)
    cc = m.cursor()
    cc.execute('CREATE TABLE IF NOT EXISTS test_max_packet (col LONGTEXT)')

    # Create a string longer than max_allowed_packet.
    longstr = str(''.join(['x' for x in range(max_allowed_packet * 2)]))
    print('Long string is %s bytes: %s' % (len(longstr), longstr))
    cc.execute('INSERT INTO test_max_packet (`col`) VALUES ("%s")' % (longstr))
    m.commit()

    cc.execute('SELECT count(*) FROM test_max_packet')
    print(cc.fetchone())

    cc.execute('DROP TABLE test_max_packet')
    m.commit()

use_pymsql()
