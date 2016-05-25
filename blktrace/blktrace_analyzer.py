#################################################################################################
# <copyright file="blktrace_analyzer.py" company="HPCAN Lab., Sharif University of Technology"> #
#                                                                                               #
# Copyright (C) 2016 Arash Tavakkol, Ata Fatahi Baarzi, Mahdi Khosravi                          #
#                                                                                               #
# This program is free software: you can redistribute it and/or modify                          #
# it under the +terms of the GNU General Public License as published by                         #
# the Free Software Foundation, either version 3 of the License, or                             #
# (at your option) any later version.                                                           #
#                                                                                               #
# This program is distributed in the hope that it will be useful,                               #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                                #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                                 #
# GNU General Public License for more details.                                                  #
#                                                                                               #
# You should have received a copy of the GNU General Public License                             #
# along with this program.  If not, see http://www.gnu.org/licenses/.                           #
# </copyright>                                                                                  #
# <summary>                                                                                     #
# {one line to give the program's name and a brief idea of what it does.}                       #
#                                                                                               #
# Email: 
#       arasht@ipm.ir                                                                          #
#       afbcesh91@gmail.com                                                                    #
#       mahdi0khosravi@gmail.com                                                               #
# </summary>                                                                                    #
################################################################################################

import os
import sys
import glob
from math import *
from xlrd import open_workbook
from xlwt import Workbook, Formula
from tempfile import TemporaryFile
class LASet:
    def __init__(self, size):
        self.size = size
        self.maxValue = 0
        self.minValue = inf
        self._count = 0
        slef._mySet = []
        self._mySet.append([])
    def Add(self, value):
        if (len(sef._mySet[-1]) > 90000000):
            self._mySet.append([])
        for i in range(len(self._mySet)):
            if value in self._mySet[i]:
                return
        if (value > self.maxValue):
            self.maxValue = value
        if (value < self.minValue):
            self.minValue = value
        self._mySet[-1].append(value)
        self._count+=1
    def Clear(self):
        self._mySet.clear()
        self._count = 0

    def Count(self):
        return self._count
    def MAXLBA(self):
        return self.maxValue
    def MINLBA(self):
        return self.minValue
            
def analyseBLKTraceFile(trace):
    #def params
    maxImportantValue = 2048
    sectorSizeInBytes = 512
    quantifyAddressesToPageSize = False
    currentPageSize = 2    
    
    LBAStartSet =  LASet(inf)
    LPAStartSet =  LASet(inf)
    LBAAllSet =  LASet(inf)
    LPAAllSet =  LASet(inf)
    LPAReadSet =  LASet(inf)
    LPAWriteSet =  LASet(inf)

    frequencyValues =[0]*(maxImportantValue + 2)
    frequencyValuesR =[0]*(maxImportantValue + 2)
    frequencyValuesW =[0]*(maxImportantValue + 2)

    maxBlockAddressInSector = 0
    minBlockAddressInSector = inf    
    
    writeCount = 0
    readCount = 0
    totalReqs = 0
    totalDataTransfer = 0
    totalReadDataTransfer = 0
    totalWriteDataTransfer = 0
    mode = 0
    modeR = 0
    modeW = 0
    avgInterarrivalTime = 0
    avgInterarrivalTimeR = 0
    avgInterarrivalTimeW = 0
    avgReqSize = 0
    avgReqSizeR = 0
    avgReqSizeW = 0    
    maxDeviceID = 0
    GMEAN = 0
    GMEANW = 0
    GMEANR = 0

    currentTime = 0
    lastTime = 0
    requestSize = 0
    currentAddress = 0
    subPagePerPage = currentPageSize

    LBAStartSet.Clear()
    LBAAllSet.Clear()
    LPAStartSet.Clear()
    LPAAllSet.Clear()
    LPAReadSet.Clear()
    LPAWriteSet.Clear()

    
    with open(trace,"r") as traceFile:
        for line in traceFile:
            tokenizedLine =[token for token in line.split()]
            currentTime = tokenizedLine[1]
            currentAddress = tokenizedLine[3]
            requestSize = tokenizedLine[4]
            deviceID = tokenizedLine[2]

            if(lastTime == 0):
                lastTime = currentTime
            #Read Requests
            if('R' in line[-1]):
                LSA = currentAddress
                SectorCount = requestSize
                lastLSA = LSA + SectorCount - 1
                while (LSA < lastLSA):
                    LPAReadSet.Add(LSA / subPagePerPage)
                    LSA+=1

                totalReadDataTransfer += requestSize
                if (requestSize < 1):
                   GMEANR += log(1)
                else:
                    GMEANR += log(requestSize)
                avgInterarrivalTimeR += currentTime - lastTime
                if (int(requestSize) <= maxImportantValue):
                    frequencyValuesR[requestSize]+=1
                else:
                    frequencyValuesR[maxImportantValue + 1]+=1
                readCount+=1
            elif('W' in line[-1]):
                LSA = currentAddress
                SectorCount = requestSize
                lastLSA = LSA + SectorCount - 1
                while ( LSA < lastLSA):
                    LPAWriteSet.Add(LSA / subPagePerPage)
                    LSA+=1
                totalWriteDataTransfer += requestSize
                if (requestSize < 1):
                   GMEANW += log(1)
                else:
                    GMEANW += log(requestSize)
                avgInterarrivalTimeW += currentTime - lastTime

                if ((int) requestSize <= maxImportantValue):
                    frequencyValuesW[requestSize]+=1
                else:
                    frequencyValuesW[maxImportantValue + 1]+=1
                
                writeCount+=1
            #Total Reqs
            LSA = (uint)currentAddress
            SectorCount = (uint)requestSize;
            lastLSA = LSA + SectorCount - 1
            LBAStartSet.Add(LSA)
            LPAStartSet.Add(LSA / subPagePerPage);
            while (LSA < lastLSA):
                LBAAllSet.Add(LSA)
                LPAAllSet.Add(LSA / subPagePerPage)
                LSA+=1
            totalDataTransfer += requestSize
            if (requestSize < 1):
                GMEAN += log(1)
            else:
                GMEAN += log(requestSize)
            avgInterarrivalTime += currentTime - lastTime
            if (maxBlockAddressInSector < currentAddress):
                maxBlockAddressInSector = currentAddress
            if (minBlockAddressInSector > currentAddress):
                minBlockAddressInSector = currentAddress
            if ((int) requestSize <= maxImportantValue):
                frequencyValues[requestSize]+=1
            else:
                frequencyValues[maxImportantValue + 1]+=1            
            totalReqs+=1
            lastTime = currentTime
            
    traceFile.close()
    if (readCount == 0):
        GMEANR = 0
        avgInterarrivalTimeR = 0
        avgReqSizeR = 0
    else:
        GMEANR = pow(2.718, (GMEANR / readCount))
        avgInterarrivalTimeR /= readCount
        avgReqSizeR = float(totalReadDataTransfer / readCount)

    if (writeCount == 0):
        GMEANW = 0
        avgInterarrivalTimeW = 0
        avgReqSizeW = 0
    else:
        GMEANW = pow(2.718, (GMEANW / writeCount))
        avgInterarrivalTimeW /= writeCount
        avgReqSizeW = float(totalWriteDataTransfer / writeCount)

    if (totalReqs == 0):
        GMEAN = 0
        avgInterarrivalTime = 0
        avgReqSize = 0
    else:
        GMEAN = pow(2.718, (GMEAN / totalReqs))
        avgInterarrivalTime /= readCount
        avgReqSize = float(totalDataTransfer / totalReqs)
    
    statFile = open("blktrace_statFile","w")
    statFile.write("MaxLBA (in sector)                 \t= " + str(LBAAllSet.MAXLBA())+"\n")
    statFile.write("MaxLBA (in sector)                 \t= " + str(LBAAllSet.MINLBA())+"\n")
    statFile.write("Accessed LPAs (for " + str(currentPageSize / 2 )+ "KB page size)   \t= " + str(LPAAllSet.Count())+"\n")
    statFile.write("Accessed LPAs for Reads            \t= " + str(LPAReadSet.Count())+"\n")
    statFile.write("Accessed LPAs  for Writes          \t= " + str(LPAWriteSet.Count())+"\n")
    statFile.write("Total Number of Requests           \t= " + str(totalReqs)+"\n")
    statFile.write("Number of Reads                    \t= " + str(readCount)+"\n")
    statFile.write("Reads Percentage                   \t= " + str(float(float(readCount) / float(totalReqs)) * 100) + "%\n")
    statFile.write("Writes Percentage                   \t= " + str(float(float(writeCount) / float(totalReqs)) * 100) + "%\n")
    statFile.write("Total Data Transer                 \t= " + str(float(float(totalDataTransfer) / float(1024 * 1024 * 2)))+"GB\n")
    statFile.write("Total Read Data Transer                 \t= " + str(float(float(totalReadDataTransfer) / float(1024 * 1024 * 2)))+"GB\n")
    statFile.write("Total Read Data Transer                 \t= " + str(float(float(totalWriteDataTransfer) / float(1024 * 1024 * 2)))+"GB\n")
    statFile.write("Average IO/S                       \t= " + str(float(float(totalReqs * 1000000000) / float(lastTime)))+"\n")
    statFile.write("Average Read IO/S                       \t= " + str(float(float(readCount * 1000000000) / float(lastTime)))+"\n")
    statFile.write("Average Write IO/S                       \t= " + str(float(float(writeCount * 1000000000) / float(lastTime)))+"\n")
    statFile.write("Request Size Average               \t= " + str(float( avgReqSize / 2))+"KB\n")
    statFile.write("Request Size Average (Read)               \t= " + str(float( avgReqSizeR / 2))+"KB\n")
    statFile.write("Request Size Average  (Write)             \t= " + str(float( avgReqSizeW / 2))+"KB\n")
    statFile.write("Request Size GMEAN               \t= " + str(float( GMEAN / 2))+"KB\n")
    statFile.write("Request Size GMEAN (Read)               \t= " + str(float( GMEANR / 2))+"KB\n")
    statFile.write("Request Size GMEANW (Write)              \t= " + str(float( GMEANW / 2))+"KB\n")

    for  i in range(1, maxImportantValue + 2):
        if (frequencyValues[i] > frequencyValues[mode]):
            mode = i
    for i in range(1, maxImportantValue + 2):
        if (frequencyValuesR[i] > frequencyValuesR[modeR]):
            modeR = i
    for i in range(1, maxImportantValue + 2):
        if (frequencyValuesW[i] > frequencyValuesW[modeW]):
            modeW = i

    if (mode != maxImportantValue + 1):
        statFile.write("Request Size Mode                   \t= " + str(mode / 2 )+ "KB\n")
    else:
        statFile.write("Request Size Mode                   \t= " + ">1MB\n")
    if (modeR != maxImportantValue + 1):
        statFile.write("Request Size Mode (Read)            \t= " + str(modeR / 2) + "KB\n")
    else:
        statFile.write("Request Size Mode (Read)            \t= " + ">1MB\n")
    if (modeW != maxImportantValue + 1):
        statFile.write("Request Size Mode (Write)           \t= " + str(modeW / 2) +"KB\n")
    else:
        statFile.write("Request Size Mode (Write)           \t= " + ">1MB\n")

    statFile.write("Average Interarrival Time           \t= " + str(float(avgInterarrivalTime / 1000000)) + "(ms)\n")
    statFile.write("Average Interarrival Time           \t= " + str(float(avgInterarrivalTimeR / 1000000)) + "(ms)\n")
    statFile.write("Average Interarrival Time           \t= " + str(float(avgInterarrivalTimeW / 1000000)) + "(ms)\n")
    statFile.write("Request Size Frequency:\n")
    for  i in range(1, maxImportantValue + 1):
        statFile.write(str(i) + "\t= " + str(frequencyValues[i])+"\n")
    statFile.write(">>>" + str(maxImportantValue) + "\t= " + str(frequencyValues[maxImportantValue + 1]))
    statFile.Close()

def create_excel(traceFile):
    book = Workbook()
    mySheet = book.add_sheet('salam')
    mySheet.write(0,1 , "PID")
    mySheet.write(0,2 , "Arrival Time")
    mySheet.write(0,3 , "Address")
    mySheet.write(0,4 , "Size")
    with open(traceFile, 'r') as inF:
        lineIndex = 1
        for line in inF:
            tolenizedLine = [token for token in line.split()]
            mySheet.write(lineIndex,0,tokenizedLine[0])					
            mySheet.write(lineIndex,1,tokenizedLine[1])
            mySheet.write(lineIndex,2,tokenizedLine[3])
            mySheet.write(lineIndex,3,tokenizedLine[4])        
            lineIndex+=1
                                    
    book.save(traceFile + '.xls')
    book.save(TemporaryFile())
    xf = glob.glob("*.xls")[0]        
    finalBook = Workbook()
    finalSheet = finalBook.add_sheet('FINAL')
    finalSheet.write(0,1 , "PID")
    finalSheet.write(0,2 , "Arrival Time")
    finalSheet.write(0,3 , "Address")
    finalSheet.write(0,4 , "Size")
    finalSheet.write(0,5 , "Interval")
    curBook = open_workbook(xf)
    curSheet = curBook.sheet_by_index(0)
    pids = curSheet.col_values(1,1)
    arrivalTimes = curSheet.col_values(2,1)
    addresses = curSheet.col_values(3,1)
    sizes = curSheet.col_values(4,1)
    for i in range(len(pids)):
            curSheet.write(i+1,1,pids[i])
            curSheet.write(i+1,2,arrivalTimes[i])
            curSheet.write(i+1,3,addresses[i])
            curSheet.write(i+1,4,sizes[i])

    for i in range(len(opsVals)):
        opsSheet.write( i+1 ,5, Formula('SUB(C'+str(i+2)+':C'+str(i+1)+')' ))
     
    finalBook.save('FINAL_BLKTRACE_EXCEL.xls')
    finalBook.save(TemporaryFile())


    for xf in xlFiles:
        os.remove(xf)

if (__name__ == '__main__'):

    if(not ('-i' in sys.argv)):
        print("Usage: -i inputFileName")
        print("Error: please specify input file")
        return
    else:
        analyseBLKTraceFile(sys.argv[1])
        create_excel(sys.argv[1])
        print("successfully done: see blktrace_statFile and FINAL_BLKTRACE_EXCEL.xls")
        return
