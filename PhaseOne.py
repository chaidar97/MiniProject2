import re
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
    print("Data Parsed")

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
            ads.append(id + ":" + line.replace("\n", "").replace(":", ""))
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
        termFile.write(t + "\n")
    # write the data to files
    for i in range(len(pdates)):
        data = priceList[i].split(":")
        priceFile.write(priceList[i] + "\n")
        pdatesFile.write(pdates[i] + "\n")
        adsFile.write(ads[i] + "\n")


main()
