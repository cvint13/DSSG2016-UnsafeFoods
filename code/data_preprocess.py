import requests
import re
from time import sleep


def getUPC10(upc_list):
    """
    Returns list of the unique 10-digit UPCs from list of UPCs
    First find unique UPCs by inserting into a set, and then build a list of
    UPCs that are of length 10
    Parameters
    ----------
    upc_list: List of UPC numbers as strings
    Returns
    -------
    upc_10: List of unique UPCs that have length 10
       
    SAMPLE USAGE
    ------------    
    upc_list = ['030243507998', '3024386680', '3024386681', '3024386687',
    '3024386683', '3024302860'] 
    getUPC10(upc_list)
    """
    upc_10 = []
    upc_set = set(upc_list)
    for u in upc_set:
        if len(u) == 10:
            upc_10.append(u)
    return upc_10


def checkDigit(s):
    """
    Calculates the 12th 'check digit' from the 11 digit UPC ('Number system
    digit' + 10-digit UPC)
    
    Parameters
    ----------
    s: 11-digit long string
    
    Returns
    -------
    0, 10-remainder: check digit for an 11-digit UPC number
    (1) Add digits in odd positons
    (2) Multiply result of (1) * 3
    (3) Add digits in even positions
    (4) Add results of (2) and (3)
    (5) Find remainder of (4)
    (6) If (5) is 0, return 0, otherwise return 10 - (5)
    
    SAMPLE USAGE
    ------------
    # check digit is 8
    checkDigit("03024350799")
    """
    even_sum = 0
    odd_sum = 0
    for i in range(len(s)):
        if i % 2 == 0:
            odd_sum += int(s[i])
        else: 
            even_sum += int(s[i])
    remainder = (odd_sum * 3 + even_sum) % 10
    if remainder == 0:
        return 0
    else:
        return 10 - remainder

    
def UPC10to12(s):
    """
    Returns list of possible 12-digit UPCs for a 10-digit UPC given each
    possible 'Number system digit' in order of likelyhood 
    given overall frequency distribution of first digits 
    (No more than one UPC in the list actually exists)
    Parameters
    ----------
    s: String containing a 10-digit UPC
    Returns
    -------
    upc12_possible_list: List of possible twelve-digit UPCs with different
    start digits and a valid check digit
    SAMPLE USAGE
    ------------
    #first element of the returned list '030243507998' is the actual UPC
    UPC10to12("3024350799")
    """
    upc12_possible_list = []
    for num in [0, 7, 8, 6, 3, 1, 9, 2, 4, 5]:
        upc_11 = str(num)+s
        upc_12 = upc_11 + str(checkDigit(upc_11))
        upc12_possible_list.append(upc_12)
    return upc12_possible_list


def getASIN(upc):
    """
    Query UPCtoASIN.com to determine ASIN from UPC.
    If the UPC is not twelve digits, the function will throw a ValueError.
    
    If the UPC is twelve digits, query the website and retrieve ASIN, then wait
    one second (per API rate limit).
    
    Parameters
    ----------
    upc: String containing a twelve-digit UPC
    
    Returns
    -------
    response.text: String containing either a valid Amazon ID (ASIN) or
    "UPCNOTFOUND"
    SAMPLE USAGE
    ------------
    getASIN("876063002233")     # 'B001BCH7KM'
    getASIN("8760630022")       # Throws an error
    """
    if len(upc) != 12:
        raise ValueError("UPC must be twelve digits long")
    else:
        url = "http://upctoasin.com/" + upc
        response = requests.get(url)
        sleep(1)                # Sleep for one second
        return(response.text)

    
def searchPossUPCs(upc_list):
    """
    Look for an Amazon ID from a list of possible UPCs.
    For a list of candidate UPCs generated by UPC10to12, loop through and
    search for an Amazon ID (ASIN). If an ASIN is found, the loop ends and the
    ASIN is returned, otherwise the function returns "UPCNOTFOUND".
    This function throws an error if it hits a UPC whose length is not 12.
    Parameters
    ----------
    upc_list: List of twelve-digit UPCs generated by UPC10to12()
    Returns
    -------
    res: String containing either a valid Amazon ID (ASIN) or
    "UPCNOTFOUND"
    SAMPLE USAGE
    ------------
    a = ['076063002237', '176063002234', '676063002239', '776063002236',
         '876063002233']
    searchPossUPCs(a)
    # Throws an error because b[2] is not 12 digits
    b = ['076063002237', '176063002234', '124542', '676063002239',
         '776063002236', '876063002233']
    searchPossUPCs(a)
    """
    res = None
    for code in upc_list:
        if len(code) != 12:
            raise ValueError("UPC must be twelve digits long.")
        else:
            res = getASIN(code)
            if res != "UPCNOTFOUND":
                break
    return(res)


def UPCtoASIN(upc):
    """
    Attempt to find an ASIN for a given UPC.
    First, remove non-numeric characters (like dashes) from the UPC.
    If UPC is ten digits, use Miki's UPC10to12 function to generate possible
    twelve-digit UPCs. Then, run those through searchPossUPCs to find an ASIN
    if one exists.
    
    If UPC is eleven digits, calculate last digit using checkDigit function and append it to string.
    Then run this 12-digit string through getASIN. 
    (Assumes that 11-digit strings are missing last digit, rather than first digit)
    If UPC is twelve digits, run through getASIN directly.
    If UPC is neither ten, nor eleven, nor twelve digits, return "UPClength-n" where n is
    the number of digits.
    Parameters
    ----------
    upc: String containing a possible UPC. Can be any length and can contain
    non-numeric characters
    Returns
    -------
    res: String containing a) Amazon ID (if found), b) "UPCNOTFOUND", or c)
    "UPClength-n" where n is the number of digits
    SAMPLE USAGE
    ------------
    print(UPCtoASIN("125483562"))          # UPClength-9
    print(UPCtoASIN("5143549862"))         # UPCNOTFOUND
    print(UPCtoASIN("7606300223"))         # B001BCH7KM
    print(UPCtoASIN("76063-00223"))        # B001BCH7KM
    print(UPCtoASIN("08606920030"))        # B004KT7UQY
    print(UPCtoASIN("125846523692"))       # UPCNOTFOUND
    print(UPCtoASIN("0-86069-20030-8"))    # B004KT7UQY
    # Loop over multiple UPCs
    upc = ["876063002233", "013000006408", "895296001035", "0-86069-20030-8"]
    for i in upc:
        print(UPCtoASIN(i))
    """
    upc_dig = re.sub("[^0-9]", "", upc)  # Remove non-numeric characters first
    res = None
    if len(upc_dig) == 10:
        upc_12_list = UPC10to12(upc_dig)
        res = searchPossUPCs(upc_12_list)
    elif len(upc_dig) == 11:
        upc_12 = upc_dig + str(checkDigit(upc_dig))
        res = getASIN(upc_12)
    elif len(upc_dig) == 12:
        res = getASIN(upc_dig)
    else:
        res = "UPClength-" + str(len(upc_dig))
    return(res)

def UPC10(upc, event_upcs12):
    upcs = list()
    for u12 in event_upcs12:
        if u12.find(upc) != -1:
            upcs.append(u12)
            return upcs
    for u12 in event_upcs12:
        if u12.find(upc[0:4]) != -1:
            u11_try = u12[0]+upc
            u12 =  u11_try + str(checkDigit(u11_try))
            upcs.append(u12)
            return upcs
    upcs = UPC10to12(upc)
    return upcs

def UPC11(upc, event_upcs12):
    upcs = list()
    for u12 in event_upcs12:
        if (u12.find(upc)) != -1:
            upcs.append(u12)
            return upcs
    u12 = upc + str(checkDigit(upc))
    upcs.append(u12)
    return upcs

def UPC13(upc):
    upcs = list()
    if upc[:2] == '00':
        u11_try = upc[1:-1]
        u12 =  u11_try + str(checkDigit(u11_try))
        upcs.append(u12)
        return upcs
    else:
        u11_try1 = upc[1:-1]
        u11_try2 = upc[2:]
        u12_1 =  u11_try1 + str(checkDigit(u11_try1))
        u12_2 =  u11_try2 + str(checkDigit(u11_try2))
        upcs.append(u12_1)
        upcs.append(u12_2)
        return upcs

def UPC14(upc):
    upcs = list()
    u11_try = upc[2:-1]
    u12 =  u11_try + str(checkDigit(u11_try))
    upcs.append(u12)
    return upcs

def getUPCS(upc, event_upcs12):
    """
    for rownum in range(enforce.shape[0]):
        for upc in enforce.upcs[rownum]:
            getUPCS(upc, enforce.event_upc12[rownum])
    """
    if len(upc) == 10:
        return UPC10(upc, event_upcs12)
    elif len(upc) == 11:
        return UPC11(upc, event_upcs12)
    elif len(upc) == 12:
        upc_list = [upc]
        return upc_list
    elif len(upc) ==13:
        return UPC13(upc)
    elif len(upc) ==14:
        return UPC14(upc)