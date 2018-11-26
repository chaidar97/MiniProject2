import re, os
def main():
    termFile = "terms.txt"
    priceFile = "prices.txt"
    adsFile = "ads.txt"
    pdatesFile = "pdates.txt"
    answer = input("Would you like load the db or dump it (L/D)? ")
    phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile)





def phaseTwo(answer, termFile, priceFile, adsFile, pdatesFile):
    if(answer.lower() == "l"):
        # Sort the files and copy them into new text files
        os.system("sort -u -o " + termFile + " " + termFile)
        os.system("sort -n -u -o " + priceFile + " " + priceFile)
        os.system("sort -u -o " + adsFile + " " + adsFile)
        os.system("sort -u -o " + pdatesFile + " " + pdatesFile)
        # Split and put data on new line
        os.system("sed -i 's/:/\\n/g' " + termFile)
        os.system("sed -i 's/:/\\n/g' " + priceFile)
        os.system("sed -i 's/:/\\n/g' " + adsFile)
        os.system("sed -i 's/:/\\n/g' " + pdatesFile)
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



main()