import re, os

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
    while True:
        query = input("Enter Query(type Exit to stop): ")
        if (query.lower() == "exit"):
            return
        phaseThree(query)


def phaseThree(query):
    # Stores keyword searches:
    keywords = []
    # Stores title/desc lookups:
    desc = ""
    # Boolean indicating if the wildcard is present
    wildCard = False
    # Splits the query up to parse it
    words = re.split("( |=|<=|>=|>|<)", query)
    # Remove empty strings/white spaces
    words = [x for x in words if x]
    words = [x for x in words if x.strip()]
    for i in range(len(words)):
        # Check for date/price conditions
        if(words[i].lower().__contains__("date") or words[i].lower().__contains__("price")):
            # Makes sure the price is numerical
            if(words[i].__contains__("price")):
                try:
                    int(words[i+2])
                except:
                    print("Price must be compared to a number.")
                    return
            #Store the keyword search
            arr = [words[i].lower(), words[i+1].lower(), words[i+2].lower()]
            keywords.append(arr)
        # Stores keywords for cat/location
        elif(words[i].lower().__contains__("cat") or words[i].lower().__contains__("location")):
            # Checks if the operator is invalid
            if(words[i+1] != "="):
                print("Invalid operator on cat/location.")
                return
            else:
                arr = [words[i].lower(), words[i + 1].lower(), words[i + 2].lower()]
                keywords.append(arr)
        #TODO: Output stuff
        elif(words[i].lower().__contains__("output")):
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
                # Checks if multiple fields are entered(invalid)
                if (len(desc) > 0):
                    print("Invalid entry: Multiple title/description fields")
                    return
                elif (words[i].endswith("%")):
                    wildCard = True
                desc = str(words[i])


    print(keywords)
    print("Lookup: " + desc)
    print("Wildcard: " + str(wildCard))




def phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile):
    if(answer.lower() == "l"):
        # Sort the files and copy them into new text files
        #os.system("sort -u -o " + termFile + " " + termFile)
        os.system("sort -n -u -o " + priceFile + " " + priceFile)
        os.system("sort -u -o " + adsFile + " " + adsFile)
        os.system("sort -u -o " + pdatesFile + " " + pdatesFile)
        # Create the indices
        os.system("db_load -T -t btree -f " + termFile + " te.idx")
        os.system("db_load -T -t btree -f " + priceFile + " pr.idx")
        os.system("db_load -T -t hash -f " + adsFile + " ad.idx") # Hash index
        os.system("db_load -T -t btree -f " + pdatesFile + " da.idx")
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
    for line in file:
        id = ""
        # get the ID from the line
        try:
            result = re.search('<aid>(.*)</aid>', line)
            id = result.group(1)
            ads.append(id + ":" + line)
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
    # create the files
    termFile = open(termsName, "w")
    priceFile = open(priceName, "w")
    pdatesFile = open(pdatesName, "w")
    adsFile = open(adsName, "w")
    # Size of this file is different, so append values to it
    for t in termList:
        termFile.write(t + "\n")
    # write the data to files
    for i in range(len(pdates)):
        priceFile.write(priceList[i] + "\n")
        pdatesFile.write(pdates[i] + "\n")
        adsFile.write(ads[i] + "\n")




def checkArray(array, word):
    inArray = False
    for n in array:
        if(word in n):
            inArray = True
    return inArray

main()