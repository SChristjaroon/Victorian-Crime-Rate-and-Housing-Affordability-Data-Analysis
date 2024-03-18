import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np
import os
from scipy import stats
from scipy.stats import pearsonr

# Generate total incidents for each subject per year
def genTotalIncidents(datasetCrime):
    dfCrime = pd.read_excel(datasetCrime, engine="openpyxl", sheet_name=3)
    dfCrime = dfCrime.drop("Postcode", axis=1).groupby(["Year", "Suburb/Town Name"]).sum()

    # Write to disk total crime incidents (separated by year)
    with pd.ExcelWriter("Datasets/Incidents_Per_Suburb_Year_Separated.xlsx") as writer:
        for i in range(2011, 2020):
            dfCrime.loc[i].to_excel(writer, sheet_name=str(i))

# Merge prices and crimes
def mergePricesAndIncidents(datasetPrices):
    dfPrice = pd.read_excel(datasetPrices, skiprows=[0, 2])
    dfPrice.columns.values[0] = "Suburb/Town Name"
    # Get the crime data written by genTotalIncidents
    with pd.ExcelWriter("Datasets/Incidents_and_Prices_Per_Suburb_Year_Separated.xlsx") as writer:
        for i in range(2011, 2020):
            # Merge prices and crimes data
            prices = dfPrice[["Suburb/Town Name", i]].copy()
            crimes = pd.read_excel("Datasets/Incidents_Per_Suburb_Year_Separated.xlsx", engine="openpyxl", sheet_name=str(i))
            prices["Suburb/Town Name"] = prices["Suburb/Town Name"].apply(
                lambda x: re.sub(r"( \([A-Za-z ]+\))", "", x).lower().capitalize())
            crimes["Suburb/Town Name"] = crimes["Suburb/Town Name"].apply(
                lambda x: x.lower().capitalize())
            merged = prices.merge(crimes, how="inner", on="Suburb/Town Name")

            # Cleanse house prices data for the current year
            merged.rename(columns={i:"Median House Price"}, inplace=True)
            merged = merged[merged["Median House Price"] != "-"]
            merged["Median House Price"] = merged["Median House Price"].apply(lambda x: int(x))

            # Write the merged data
            merged.to_excel(writer, sheet_name=str(i), index=False)

# Data wrangle population/suburbs data and create crime rates
def genCrimeRates(datasetSuburbs, datasetPopulations):
    # Get the list of each suburb and their suburb code
    dfSuburbs = pd.read_csv(datasetSuburbs)
    dfSuburbs.columns.values[0] = "Suburb/Town Name"
    dfSuburbs.columns.values[1] = "Suburb Code"

    # Cleanse the suburb names
    dfSuburbs["Suburb/Town Name"] = dfSuburbs["Suburb/Town Name"].apply(
        lambda x: re.sub(r"( \([A-Za-z .]+\))", "", x).lower().capitalize())

    # Get the population of suburbs by their code
    dfPopulations = pd.read_csv(datasetPopulations)
    dfPopulations.columns.values[0] = "Suburb Code"
    dfPopulations.columns.values[3] = "Total Pop"

    # Take only suburbs with a measured population (for both males and females)
    dfPopulations = dfPopulations.loc[(dfPopulations["Tot_P_M"] > 0) &
                                      (dfPopulations["Tot_P_F"] > 0), 
                                      ["Suburb Code", "Total Pop"]]

    # Cleanse the suburb codes to match dfSuburbs
    dfPopulations["Suburb Code"] = dfPopulations["Suburb Code"].apply(lambda x: int(x[3:]))

    # Merge the suburb populations with their names
    dfSubPops = dfSuburbs.merge(dfPopulations, how="inner", on="Suburb Code")

    # Merge with the wrangled data from mergePricesAndIncidents
    with pd.ExcelWriter("Datasets/Crime_Per_Suburb_Per_Year.xlsx") as writer:
        for i in range(2011, 2020):
            dfCrime = pd.read_excel("Datasets/Incidents_and_Prices_Per_Suburb_Year_Separated.xlsx", 
                                    engine="openpyxl", 
                                    sheet_name=str(i))
            dfCrime = dfCrime.merge(dfSubPops, how="inner", on="Suburb/Town Name")
            # Calculate the crime rate per 1000 people (and remove outliers, due to population disrepancies)
            dfCrime["Crime Rate Per 1000"] = (dfCrime["Incidents Recorded"] / dfCrime["Total Pop"]) * 1000
            dfCrime = dfCrime[np.abs(stats.zscore(dfCrime["Crime Rate Per 1000"])) < 3]
            dfCrime.to_excel(writer, sheet_name=str(i), index=False)

# Generate the total incidents and crime rates per LGA per year
def genLocalCrime(datasetCrime):
    dfCrime = pd.read_excel(datasetCrime, engine="openpyxl", sheet_name=1)
    dfCrime = dfCrime[dfCrime["Local Government Area"] != "Total"]\
        .drop(["Year ending", "Police Region"], axis=1)\
        .dropna(axis=1, how='all')\
        .sort_values(by="Local Government Area")\
        .groupby("Year")
    
    # output the crime data to an excel file
    with pd.ExcelWriter("Datasets/Crime_Per_Local_Area_Year_Separated.xlsx") as writer:
        for year in dfCrime.groups:
            dfCrime.get_group(year).drop("Year", axis=1).to_excel(writer, sheet_name=str(year), index=False)

# Generate the property sales data for each LGA per year
def genLocalProperty(datasetProperty):
    fileData = pd.ExcelFile(datasetProperty)
    numRows = fileData.book.sheet_by_index(0).nrows
    columnNames = ["Local Government Area", "Num Sales", "Median Price", "Mean Price"]
    yearDfs = [pd.DataFrame(columns=columnNames)]*10

    # Get the start and end positions of each LGA table
    tableStarts = fileData.parse(0, usecols='F').dropna(axis=0, how='any').index.values[3:]
    tableStarts = [x+1 for x in tableStarts]
    tableEnds = fileData.parse(0, usecols='E')
    tableEnds = tableEnds[tableEnds.iloc[:, 0] == 2020].index.values[3:]
    tableEnds = [x+2 for x in tableEnds]

    # Go through each LGA and parse its 2011-2020 data for houses
    for index in range(len(tableStarts)):
        # Get the table data
        df = fileData.parse(0, skiprows=tableStarts[index], skipfooter=numRows-tableEnds[index])\
            .dropna(axis=1, how='all')\
            .dropna(axis=0, how='all')
        # Get the table name (above the table data)
        name = fileData.parse(
            0, skiprows=tableStarts[index]-9, 
            skipfooter=numRows-(tableStarts[index]-8), usecols='B')\
            .columns.values[0]
        # Add the data for each year (2011-2020) to the respective year dataframes
        for i in range(10):
            test = []
            test.append(name)
            test.extend(value for value in df.iloc[i+22, 1:4])
            yearDfs[i] = yearDfs[i].append(pd.Series(test, index=columnNames), ignore_index=True)
    
    # output the year data to an excel file
    with pd.ExcelWriter("Datasets/Property_Per_Local_Area_Year_Separated.xlsx") as writer:
        for i in range(10):
            yearDfs[i].to_excel(writer, sheet_name=str(2011+i), index=False)

# Merge the crimes vs house sale frequencies data for local government areas
def mergePropertyAndCrime():
    with pd.ExcelWriter("Datasets/Local_Crime_And_Property_Per_Year.xlsx") as writer:
        for i in range(2011, 2021):
            dfCrime = pd.read_excel("Datasets/Crime_Per_Local_Area_Year_Separated.xlsx", engine="openpyxl", sheet_name=str(i))
            dfProperty = pd.read_excel("Datasets/Property_Per_Local_Area_Year_Separated.xlsx", engine="openpyxl", sheet_name=str(i))
            dfProperty["Local Government Area"] = dfProperty["Local Government Area"].apply(lambda x: x.rsplit(' ', 1)[0].lower().capitalize())
            dfCrime["Local Government Area"] = dfCrime["Local Government Area"].apply(lambda x: x[1:].replace('-', ' ').lower().capitalize())
            merged = dfCrime.merge(dfProperty, how='inner', on='Local Government Area')
            merged.to_excel(writer, sheet_name=str(i), index=False)

# Create plots for each year
def scatterPlots(file, endYear, xName, yName, xLabel, yLabel, plotTitle, plotName):
    # Get the x axis limits
    axisLimit = 0
    for i in range(2011, endYear):
        # Read the excel data from file into a dataframe
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        # Update the axis limit
        axisLimit = axisLimit if max(df[xName]) <= axisLimit else max(df[xName])
    # Plot a scatterplot for each year
    for i in range(2011, endYear):
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        # Plot the scatterplot
        plt.scatter(df[xName], df[yName])
        plt.xlabel(xLabel)
        plt.ylabel(yLabel)
        plt.title(plotTitle + str(i))
        plt.xlim(0, axisLimit)
        # Linear trendline
        lineData = np.polyfit(df[xName], df[yName], 1)
        trendline = np.poly1d(lineData)
        plt.plot(df[xName], trendline(df[xName]), "r--")
        # Output scatter plot
        if not os.path.isdir("Plots/Scatterplots"):
            os.makedirs("Plots/Scatterplots")
        plt.savefig("Plots/Scatterplots/" + plotName + str(i) + ".png", bbox_inches="tight")
        plt.clf()

# Create a boxplot for each year
def boxPlots(file, endYear, xName, xLabel, plotTitle, plotName):
    for i in range(2011, endYear):
        # Read the excel data from file into a dataframe
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        df_sorted = df[xName].sort_values(ascending = False)
        # Plot the boxplot
        plt.boxplot(df_sorted)
        plt.xticks([1], [xLabel])
        plt.title(plotTitle + str(i))
        if not os.path.isdir("Plots/Boxplots"):
            os.makedirs("Plots/Boxplots")
        plt.savefig("Plots/Boxplots/" + plotName + str(i) + ".png", bbox_inches="tight")
        plt.clf()
        
# Create a bubble plot for each year
def bubblePlots(file, xName, yName, size, plotName, xLabel, yLabel):
    for i in range(2011, 2020):
        # Read the excel data from file into a dataframe
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        sizeCol = df[size]
        normal = sizeCol / sizeCol.max()

        plt.figure(figsize=(12, 8))
        plt.scatter(df[xName], df[yName], color='darkblue', alpha=0.5, s = normal * 2000)
        plt.xlabel(xLabel)
        plt.ylabel(yLabel)
        plt.title(plotName + str(i))
        if not os.path.isdir("Plots/Bubbleplots"):
            os.makedirs("Plots/Bubbleplots")
        plt.savefig("Plots/Bubbleplots/" + plotName + str(i) + ".png")
        plt.clf()

def lineGraphs(file, yName, plotName, xLabel, yLabel):
    years = []
    var = []
    for i in range(2011, 2020):
        # Read the excel data from file into a dataframe
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        years.append(i)
        var.append(df[yName].mean())

    plt.plot(years, var, color="red", marker='o')
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)
    plt.title(plotName)
    if not os.path.isdir("Plots/Linegraphs"):
        os.makedirs("Plots/Linegraphs")
    plt.savefig("Plots/Linegraphs/" + plotName + ".png")
    plt.clf()

# Determine the pearson correlation between two variables  
def pearson_corr(file, endYear, xName, yName, Name):
    pearson_corr_year = []
    for i in range(2011, 2020):
        # Read the excel data from file into a dataframe
        df = pd.read_excel(file, engine="openpyxl", sheet_name=str(i))
        # Convert dataframe into series
        list1 = df[xName]
        list2 = df[yName]
        # Calculate Pearson Correlation
        corr, _ = pearsonr(list1, list2)
        pearson_corr_year += [corr]
    # output to csv 
    pearson_corr_year = pd.Series((pearson_corr_year), index =[i for i in range(2011, 2020)])
    pearson_corr_year.to_csv(Name,header=False)

def suburbDataProcessing():
    # Suburb Data Wrangling
    genTotalIncidents("Datasets/Data_Tables_LGA_Criminal_Incidents_Year_Ending_December_2020.xlsx")
    mergePricesAndIncidents("Datasets/Suburb_House_final.xls")
    genCrimeRates("Datasets/Suburb_Code_To_Name.csv", "Datasets/Suburb_Populations_2016_Census.csv")

    # Suburb Plots
    scatterPlots("Datasets/Incidents_and_Prices_Per_Suburb_Year_Separated.xlsx", 2020,
             "Median House Price", "Incidents Recorded",
             "Median House Price (AUS$)", "Total Incidents Recorded",
             "Median House Prices vs Total Crime Incidents for Victorian Suburbs in ",
             "Total Incidents Scatter Plot ")
    scatterPlots("Datasets/Crime_Per_Suburb_Per_Year.xlsx", 2020,
             "Median House Price", "Crime Rate Per 1000",
             "Median House Price (AUS$)", "Crime Rate Per 1000 People",
             "Median House Prices vs Crime Rates for Victorian Suburbs in ",
              "Crime Rates Scatter Plot v3 ")
    boxPlots("Datasets/Crime_Per_Suburb_Per_Year.xlsx", 2020,
             "Median House Price",
             "Median House Price (AUS$)",
             "Median House Prices for Victorian Suburbs in ",
             "Median House Price Box Plot ")
    boxPlots("Datasets/Crime_Per_Suburb_Per_Year.xlsx", 2020,
             "Crime Rate Per 1000",
             "Crime Rate Per 1000 People",
             "Crime Rate Per 1000 People for Victorian Suburbs in ",
             "Crime Rate Per 1000 Box Plot ")
    boxPlots("Datasets/Incidents_and_Prices_Per_Suburb_Year_Separated.xlsx", 2020,
             "Incidents Recorded",
             "Total Incidents Recorded",
             "Total Crime Incidents for Victorian Suburbs in ",
             "Total Incidents Box Plot ")

    # Suburb Analysis Methods
    pearson_corr("Datasets/Incidents_and_Prices_Per_Suburb_Year_Separated.xlsx", 2020,
                 "Median House Price", "Incidents Recorded",
                 "Datasets/Pearson Correlation of Median House Price and Incidents.csv")
    
    pearson_corr("Datasets/Crime_Per_Suburb_Per_Year.xlsx", 2020,
                 "Median House Price", "Crime Rate Per 1000", 
                 "Datasets/Pearson Correlation of Median House Price and Crime Rate Per 1000.csv")

def localAreasDataProcessing():
    # LGA Data Wrangling
    genLocalCrime("Datasets/Data_Tables_LGA_Criminal_Incidents_Year_Ending_December_2020.xlsx")
    genLocalProperty("Datasets/YearlySummaryFinal.xls")
    mergePropertyAndCrime()

    # LGA Plots
    scatterPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
            "Num Sales", "Incidents Recorded",
            "Frequency of House Sales", "Total Incidents Recorded",
            "Frequency of House Sales vs Total Crime Incidents for Victorian LGAs in ",
            "LGA Total Incidents Scatter Plot ")
    scatterPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
            "Num Sales", "Rate per 100,000 population",
            "Frequency of House Sales", "Crime Rate Per 100000 People",
            "Frequency of House Sales vs Crime Rates for Victorian LGAs in ",
            "LGA Crime Rates Scatter Plot ")
    scatterPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Median Price", "Incidents Recorded",
             "Median House Price (AUS$)", "Total Incidents Recorded",
             "Median House Prices vs Total Crime Incidents for Victorian LGAs in ",
             "LGA Median House Prices vs Total Incidents Scatter Plot ")
    scatterPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Median Price", "Rate per 100,000 population",
             "Median House Price (AUS$)", "Crime Rate Per 100 000 People",
             "Median House Prices vs Crime Rates for Victorian LGAs in ",
             "LGA Median House Prices vs Crime Rates Scatter Plot ")
    boxPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Num Sales",
             "Frequency of House Sales",
             "Frequency of House Sales for Victorian LGAs in ",
             "LGA House Sales Box Plot ")
    boxPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Rate per 100,000 population",
             "Crime Rate Per 100,000 People",
             "Crime Rates for Victorian LGAs in ",
             "LGA Crime Rate Per 100000 Box Plot ")
    boxPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Incidents Recorded",
             "Total Incidents Recorded",
             "Total Crime Incidents for Victorian LGAs in ",
             "LGA Total Incidents Box Plot ")
    boxPlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
             "Median Price",
             "Median House Price (AUS$)",
             "Median House Prices for Victorian LGAs in ",
             "LGA Median House Price Box Plot ")

    bubblePlots("Datasets/Local_Crime_And_Property_Per_Year.xlsx", "Num Sales", "Incidents Recorded", 
                    "Median Price", "Bubble Plot ", "Number of House Sales", "Number of Crime Incidents")

    lineGraphs("Datasets/Local_Crime_And_Property_Per_Year.xlsx", "Incidents Recorded", 
                    "Line Graph of Incidents Recorded", "Years", "Incidents Recorded")
    
    lineGraphs("Datasets/Local_Crime_And_Property_Per_Year.xlsx", "Median Price", 
                    "Line Graph of Median House Prices", "Years", "Median House Prices")
    
    lineGraphs("Datasets/Local_Crime_And_Property_Per_Year.xlsx", "Num Sales", 
                    "Line Graph of Frequency of House Sales", "Years", "Frequency of House Sales")

    lineGraphs("Datasets/Local_Crime_And_Property_Per_Year.xlsx", "Rate per 100,000 population", 
                    "Line Graph of Crime Rate", "Years", "Crime rate per 100,000 population")


    # LGA Analysis Methods
    pearson_corr("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
                 "Num Sales", "Incidents Recorded",
                 "Datasets/Pearson Correlation of Frequency of House Sales and Incidents.csv")
    pearson_corr("Datasets/Local_Crime_And_Property_Per_Year.xlsx", 2021,
                 "Num Sales", "Rate per 100,000 population",
                 "Datasets/Pearson Correlation of Frequency of House Sales and Crime Rate Per 100000.csv")
      
def main():
    ### Data wrangle and merge crime/price data
    # Suburb Data
    suburbDataProcessing()

    # LGA Data
    localAreasDataProcessing()

if __name__ == "__main__":
    main()