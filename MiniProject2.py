import re, os, bsddb3, datetime
from bsddb3 import db

# BSDDB3 Informaton: https://docs.python.org/2/library/bsddb.html
# Just add 3 at the end its basically the same documentation :)

isOutputFull = False

def main():
    txt = input("Enter the text file: ")
    try:
        file = open(txt, "r")
    except:
        print("Invalid file.")
        return
    termFile = "terms.txt"
    priceFile = "prices.txt"
    adsFile = "ads.txt"
    pdatesFile = "pdates.txt"
    phaseOne(file, termFile, priceFile, adsFile, pdatesFile)
    print("Data parsed.")
    answer = input("Would you like load the db or dump it (L/D)? ")
    phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile)
    
    pdatesDB = db.DB()
    pdatesDB.open("da.idx", None, db.DB_BTREE)

    adsDB = db.DB() 
    adsDB.open("ad.idx", None, db.DB_HASH)

    termDB = db.DB()
    termDB.set_flags(db.DB_DUP)
    termDB.open("te.idx", None, db.DB_BTREE)
    
    priceDB = db.DB()
    priceDB.open("pr.idx", None, db.DB_BTREE)

    while True:
        query = input("Enter Query(type Exit to stop): ")
        if (query.lower() == "exit"):
            return
        phaseThree(query, adsDB, termDB, priceDB, pdatesDB)


def phaseThree(query, adsDB, termDB, priceDB, pdatesDB):
    global isOutputFull
    # Stores keyword searches:
    keywords = []
    # Stores title/desc lookups:
    desc = []
    # Boolean indicating if the wildcard is present
    wildCard = False
    # Splits the query up to parse it
    words = re.split("( |=|<=|>=|>|<)", query)
    # Remove empty strings/white spaces
    words = [x for x in words if x]
    words = [x for x in words if x.strip()]
    for i in range(len(words)):
        # Check for date/price conditions
        if(words[i].lower() == ("date") or words[i].lower() == ("price")):
            # Makes sure the price is numerical
            if(words[i].lower() == ("price")):
                try:
                    int(words[i+2])
                except:
                    print("Price must be compared to a number.")
                    return
            #Store the keyword search
            arr = [words[i].lower(), words[i+1].lower(), words[i+2].lower()]
            keywords.append(arr)
        # Stores keywords for cat/location
        elif(words[i].lower() == ("cat") or words[i].lower() == ("location")):
            # Checks if the operator is invalid
            if(words[i+1] != "="):
                print("Invalid operator on cat/location.")
                return
            else:
                arr = [words[i].lower(), words[i + 1].lower(), words[i + 2].lower()]
                keywords.append(arr)
        elif(words[i].lower() ==("output")):
            try:
                if words[i+2] == "full":
                    isOutputFull = True
                    print("Output set to full.")
                elif words[i+2] == "brief":
                    isOutputFull = False
                    print("Output set to brief.")
                else:
                    print("Output can only be full or brief.")
            except:
                print("Output can only be full or brief.")
            return
        # Will be stored as a title/description field
        else:

            # Make sure it's not a keyword search value
            if(not checkArray(keywords, words[i])):
                if (words[i].endswith("%")):
                    wildCard = True
                arr = [str(words[i]), wildCard]
                desc.append(arr)
                wildCard = False

    print(desc)
    print(keywords)
    print("Lookup: " + str(desc))
    print("----")

    # Initialize the resulting data set.
    dataSet = None
    
    #iterate through the keywords- date,price,location,cat and call the database accordingly
    for keyq in keywords:
        if keyq[0]== "date":
            if(str(keywords[0][0])=="date"):
                try:
                    datetime.datetime.strptime(str(keyq[2]), '%Y/%m/%d')
                except ValueError:
                    print("Incorrect data format, should be YYYY-MM-DD")   
                    break
                    
            dataNew = getDateQuery(keyq[1],keyq[2],pdatesDB)                
                
                
        elif keyq[0]=="price":
            dataNew=getPriceQuery(keyq[1],keyq[2],priceDB)
            
        elif keyq[0]=="location":
            dataNew=getLocationQuery(keyq[2],priceDB)
            
        elif keyq[0]=="cat":
            dataNew=getCatQuery(keyq[2],priceDB)
        
        if dataSet == None:
            # append the data to the new one if it's the first condition
            dataSet = set(dataNew)
        else:
            # intersect the data with the next query
            dataSet = dataSet.intersect(set(dataNew))

        # If the length of the set is 0, there is no possible way to add data since intersection on an empty set returns an empty set.
        if len(dataSet) == 0:
            break

    # For each of the terms to be searched
    for term in desc:
        dataNew = getTermQuery(term[0], termDB, term[1])
        if dataSet == None:
            dataSet = set(dataNew)
        else:
            dataSet = dataSet.intersection(set(dataNew))


    
    # For each item in the set, print the required data (depeneding on output type)
    if dataSet != None:
        if len(dataSet) > 0:
            for item in dataSet:
                if isOutputFull == True:
                    print([item.decode(), adsDB[item].decode()])
                else:
                    print([item.decode(), getTitleFromAd(adsDB[item].decode())])



"""
(1) a hash index on ads.txt with ad id as key and the full ad record as data - adsDB
(2) a B+-tree index on terms.txt with term as key and ad id as data - termDB
(3) a B+-tree index on pdates.txt with date as key and ad id, category and location as data - pdatesDB
(4) a B+-tree index on prices.txt with price as key and ad id, category and location as data - priceDB
"""

# Used for testing, dumps the current db into a readable format.
def dumpDB(db):
    curs = db.cursor()
    iter = curs.first()
    while (iter):
        print(iter)

        #iterating through duplicates
        dup = curs.next_dup()
        while(dup!=None):
            print(dup)
            dup = curs.next_dup()

        iter = curs.next()
 
# Get all instances of the price greater or greater and equal to the provided price.
def getPriceGreater(price, eq, db):
    cur = db.cursor()
    res = None
    
    res=cur.set_range(price.encode())

    if res == None:
        return []

    # If we include the original key, we can set the output to be the dups from price. If not we set output to empty
    if eq:
        output = getAllDupsFromPrice(price, db)
    else:
        output = []


    res = cur.next()
    if eq:
        while res != None:
            # If the price is >= to the current (should be), make the output extend and add item sfrom another list.
            if(int(res[0]) >= int(price)):
                output.extend(getAllDupsFromPrice(res[0].decode(), db))
            res = cur.next()
    else:
       while res != None:
            # If the price is >= to the current (should be), make the output extend and add item sfrom another list.
            if(int(res[0]) > int(price)):
                output.extend(getAllDupsFromPrice(res[0].decode(), db))
            res = cur.next()
        

    cur.close()
    return output    
    

# Exact same as greater than but with differing operators.
def getPriceLess(price, eq, db):
    cur = db.cursor()
    res = None
    
    res=cur.set_range(price.encode())

    if res == None:
        return []

    # If we include the original key, we can set the output to be the dups from price. If not we set output to empty
    if eq:
        output = getAllDupsFromPrice(price, db)
    else:
        output = []


    res = cur.prev()
    if eq:
        while res != None:
            # If the ads price is <= to the searching price, we add it.
            if(int(res[0]) <= int(price)):
                output.extend(getAllDupsFromPrice(res[0].decode(), db))
            res = cur.prev()
    else:
       while res != None:
            # If the ads price is < the search price, we add it.
            if(int(res[0]) < int(price)):
                output.extend(getAllDupsFromPrice(res[0].decode(), db))
            res = cur.prev()
        

    cur.close()
    return output  

# Get the price from the database.
def getPriceQuery(symbol, amnt, db):
    output = []
    if symbol == ">=":
        return getPriceGreater(amnt, True, db)
    elif symbol == ">":
        return getPriceGreater(amnt, False, db)
    elif symbol == "<=":
        return getPriceLess(amnt, True, db)
    elif symbol == "<":
        return getPriceLess(amnt, False, db)
    elif symbol == "=":
        return getAllDupsFromPrice(amnt, db)
    else:
        print("Invalid symbol provided to price query.")
        return []
        

# Get all duplicate items provided the key.
def getAllDups(key, db):
    cur = db.cursor()
    output = []
    res = cur.set(key.encode())
    if res == None:
        return []
    output.append(res)
    dup = cur.next_dup()
    while(dup != None):
        output.append(dup)
        dup = cur.next_dup()
    cur.close()
    return output

# Get all duplicate items from a given price and return the ad id.
def getAllDupsFromPrice(key, db):
    cur = db.cursor()
    output = []
    res = cur.set(key.encode())
    if res == None:
        return []

    # Append the result since its not null
    output.append(res[1].decode().split(',')[0].encode())
    dup = cur.next_dup()

    # While there is still a duplicate, run through it and get its ad id.
    while(dup != None):
        output.append(dup[1].decode().split(',')[0].encode())
        dup = cur.next_dup()
    cur.close()
    return output

# Get all instances of a keyword in the terms db title or description
def getTermQuery(keyword, db, wildcard=False):
    cur = db.cursor()
    res = None

    # If we are using a wildcard, we need to remove the % and search on the set range.
    if wildcard == True:
        keyword = keyword[:len(keyword)-1]
        res = cur.set_range(keyword.encode())
    else:
        res = cur.set(keyword.encode())
    
    # if res is None, return nothing.
    if res == None:
        return []

    output = []
    output.append(res[1])
    dup = cur.next_dup()
    while(dup != None):
        output.append(dup[1])
        dup = cur.next_dup()

    cur.close()
    return output

# Get all ad ids that have categories that match the provided key.
# DB provided must be the price DB!
def getCatQuery(key, db):
    output = []
    curs = db.cursor()
    iter = curs.first()
    while (iter):
        cat = iter[1].decode().split(',')[1]

        # If the category is the category we want to search for, add its aid to the output
        if cat == key:
            output.append(iter[1].decode().split(',')[0].encode())
        dup = curs.next_dup()
        while(dup!=None):
            cat = dup[1].decode().split(',')[1]
            if cat == key:
                output.append(iter[1].decode().split(',')[0].encode())
            dup = curs.next_dup()

        iter = curs.next()
    curs.close()
    return output

# Gets all ad's that match the location provided. Code is basically the same as cat above
def getLocationQuery(key, db):
    output = []
    curs = db.cursor()
    iter = curs.first()
    while (iter):
        cat = iter[1].decode().split(',')[2]

        # If the category is the category we want to search for, add its aid to the output
        if cat.lower() == key:
            output.append(iter[1].decode().split(',')[0].encode())
        dup = curs.next_dup()
        while(dup!=None):
            cat = dup[1].decode().split(',')[2]
            if cat.lower() == key:
                output.append(iter[1].decode().split(',')[0].encode())
            dup = curs.next_dup()

        iter = curs.next()
    curs.close()
    return output


# This should run like the price query above using >, >=, <= and <. I need to go to bed so this is a TODO:---------------------------------------------------------------------
def getDateQuery(symbol, date, db):
    print(date)
    print("---")
    print(symbol)
    return []

# Gets a title from an ads tag
def getTitleFromAd(adStr):
    result = re.search('<ti>(.*)</ti>', adStr)
    return result.group(1)


def phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile):
    if(answer.lower() == "l"):
        # Sort the files and copy them into new text files
        #os.system("sort -u -o " + termFile + " " + termFile)
        #os.system("sort -n -u -o " + priceFile + " " + priceFile)
        #os.system("sort -u -o " + adsFile + " " + adsFile)
        #os.system("sort -u -o " + pdatesFile + " " + pdatesFile)
        # Create the indices
        os.system("db_load -T -c duplicates=1 -t btree -f " + termFile + " te.idx")
        os.system("db_load -T -c duplicates=1 -t btree -f " + priceFile + " pr.idx")
        os.system("db_load -T -t hash -f " + adsFile + " ad.idx") # Hash index
        os.system("db_load -T -c duplicates=1 -t btree -f " + pdatesFile + " da.idx")
    elif(answer.lower() == "d"):
        # Dumps the data into terminal feed
        os.system("db_dump -p te.idx")
        os.system("db_dump -p pr.idx")
        os.system("db_dump -p ad.idx")
        os.system("db_dump -p da.idx")
    else:
        print("Invalid input.")




def phaseOne(file, termsName, priceName, adsName, pdatesName):
    termList = []
    pdates = []
    priceList = []
    ads = []
    # create the files
    termFile = open(termsName, "w")
    priceFile = open(priceName, "w")
    pdatesFile = open(pdatesName, "w")
    adsFile = open(adsName, "w")
    for line in file:
        id = ""
        # get the ID from the line
        try:
            result = re.search('<aid>(.*)</aid>', line)
            id = result.group(1)
            adsFile.write(id + "\n" + line.replace("\n", "") + "\n")
        except:
            pass
        #Get the title and desc words
        try:
            result = re.search('<ti>(.*)</ti>', line)
            title = result.group(1).split(" ")
            result = re.search('<desc>(.*)</desc>', line)
            desc = result.group(1).split(" ")
            # Append each word to termList
            #TODO: Still not perfect, has some invalid strings
            for i in range(len(title)):
                str = re.sub('&.+[0-9]+;', '', desc[i])
                if(len(str) > 2):
                    # Remove XML special chars
                    fin = re.sub('[^a-zA-Z0-9]+', '', str)
                    # Remove backslashes
                    fin = fin.replace("/", "")
                    termList.append(fin.lower() + ":" + id)
            for word in desc:
                str = re.sub('&.+[0-9]+;', '', word)
                if (len(str) > 2):
                    fin = re.sub('[^a-zA-Z0-9]+', '', str)
                    fin = fin.replace("/", "")
                    termList.append(fin.lower() + ":" + id)
        except:
            pass
        # Format the arrays for Pdate, prices, and ads
        try:
            result = re.search('<date>(.*)</date>', line)
            dates = result.group(1).split(" ")
            result = re.search('<cat>(.*)</cat>', line)
            cats = result.group(1).split(" ")
            result = re.search('<loc>(.*)</loc>', line)
            locs = result.group(1).split(" ")
            result = re.search('<price>(.*)</price>', line)
            prices = result.group(1).split(" ")
            #Append each row to pdates and priceList
            for d in range(len(dates)):
                pdates.append(dates[d] + ":" + id.replace("/", "") + "," + cats[d].replace("/", "") + "," + locs[d].replace("/", ""))
                priceList.append(prices[d].replace("/", "") + ":" + id.replace("/", "") + "," + cats[d].replace("/", "") + "," + locs[d].replace("/", ""))
        except:
            pass
    # Size of this file is different, so append values to it
    for t in termList:
        data = t.split(":")
        termFile.write(data[0] + "\n")
        termFile.write(data[1] + "\n")
    # write the data to files
    for i in range(len(pdates)):
        data = priceList[i].split(":")
        priceFile.write(data[0] + "\n" + data[1] + "\n")
        data = pdates[i].split(":")
        pdatesFile.write(data[0] + "\n" + data[1] + "\n")




def checkArray(array, word):
    inArray = False
    for n in array:
        if(word in n):
            inArray = True
    return inArray

main()
