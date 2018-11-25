import re, os
from bsddb3 import db

# BSDDB3 Informaton: https://docs.python.org/2/library/bsddb.html
# Just add 3 at the end its basically the same documentation :)

isOutputFull = False

def main():
    termDB = db.DB()
    termDB.open("ad.idx", None, db.DB_HASH)
    curs = termDB.cursor()
    iter = curs.first()
    while (iter):
        print(iter)
        iter = curs.next()
    curs.close()
    termDB.close()

main()
