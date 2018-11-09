import re

def main():
    txt = input("Enter the text file: ")
    file = open(txt, "r")
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
            #print(title, desc)
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
        # get the desc words
        try:
            result = re.search('<date>(.*)</date>', line)
            dates = result.group(1).split(" ")
            result = re.search('<cat>(.*)</cat>', line)
            cats = result.group(1).split(" ")
            result = re.search('<loc>(.*)</loc>', line)
            locs = result.group(1).split(" ")
            result = re.search('<price>(.*)</price>', line)
            prices = result.group(1).split(" ")
            for d in range(len(dates)):
                pdates.append(dates[d] + ":" + id + "," + cats[d] + "," + locs[d])
                priceList.append(prices[d] + ":" + id + "," + cats[d] + "," + locs[d])
        except:
            pass
    # create the files
    termFile = open("terms.txt", "w")
    priceFile = open("prices.txt", "w")
    pdatesFile = open("pdates.txt", "w")
    adsFile = open("ads.txt", "w")
    # Size of this file is different, so append values to it
    for t in termList:
        termFile.write(t + "\n")
    # write the data to files
    for i in range(len(pdates)):
        priceFile.write(priceList[i] + "\n")
        pdatesFile.write(pdates[i] + "\n")
        adsFile.write(ads[i] + "\n")



main()