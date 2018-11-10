import re, os

def main():
    txt = input("Enter the text file: ")
    file = open(txt, "r")
    termFile = "terms.txt"
    priceFile = "prices.txt"
    adsFile = "ads.txt"
    pdatesFile = "pdates.txt"
    phaseOne(file, termFile, priceFile, adsFile, pdatesFile)
    print("Data parsed.")
    answer = input("Would you like load the db or dump it (L/D)? ")
    phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile)



def phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile):
    if(answer.lower() == "l"):
        # Sort the files and copy them into new text files
        os.system("sort -u -o " + termFile + " " + termFile)
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
                str2 = re.sub('&.+[0-9]+;', '', title[i])
                if(len(str) > 2):
                    fin = re.sub('[^a-zA-Z0-9]+', '', str)
                    termList.append(fin.lower() + ":" + id)
                if (len(str2) > 2):
                    fin = re.sub('[^a-zA-Z0-9]+', '', str2)
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
                pdates.append(dates[d] + ":" + id + "," + cats[d] + "," + locs[d])
                priceList.append(prices[d] + ":" + id + "," + cats[d] + "," + locs[d])
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




main()