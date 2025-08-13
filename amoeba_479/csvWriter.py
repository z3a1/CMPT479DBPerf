import csv

def logCSVFile(fileName,labelRow ,data):
    with open(fileName, "w", newline = '') as csvFile:
        logger = csv.writer(csvFile)
        logger.writerow(labelRow)
        for row in data:
            if isinstance(row,dict):
                logger.writerow([row['base'], row['mutator']])
                # for mutator in row['mutator']:
                #     logger.writerow([row['base'], mutator])
            elif isinstance(row,list):
                logger.writerow(row)
            else:
                logger.writerow([row])