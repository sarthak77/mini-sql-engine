import csv #to read csv files
import traceback #to print error statements
import sys #to take cl input
import sqlparse #to parse sql queries

def init():
    """
    Load data in dictionary tinfo
    """

    basedr='./files/'
    with open(basedr+'metadata.txt') as f:
        metadata=f.read().splitlines() 

    tinfo={}
    newtable=False
    name=""

    for line in metadata:
        if(line=="<begin_table>"):
            newtable=True
        elif(newtable==True):
            newtable=False
            name=line
            tinfo[name]={}
        elif(line!="<end_table>"):
            tinfo[name][line]=[]

    # print(tinfo)

    for tname in tinfo:
        
        # print(tname)

        s=""
        for att in tinfo[tname]:
            s+=tname+'.'+att+','
        s=s[:-1]
        # print(s) 
        
        with open(basedr+tname+'.csv','r') as csvfile:
            csvreader=csv.reader(csvfile)

            for record in csvreader:
                ind=0
                for att in tinfo[tname]:
                    tinfo[tname][att].append(record[ind])
                    ind+=1
            
    # print(tinfo)



def checkdistinct(query):
    """
    checks if distinct keyword present or not
    """

    if(len(query)>1) and (query[1]=='distinct'):
        return True

    return False



def checkincorrect(query,distinct):
    """
    Checks if error in sql syntax
    """
    
    if not(6>=len(query)>=4):
        return True

    if(query[0]!='select'):
        return True

    if(query[2+distinct]!='from'):
        return True
    
    if(4+distinct<len(query)) and (query[4+distinct][0:6]!='where '):
        return True

    return False



def processquery(q):
    """
    Process the given query
    """

    q="select max(A),table1.B,table2.C from table1,table2 where table1.A=table2.B and C=5"
    temp=sqlparse.parse(q)[0].tokens
    query=[]
    for i in temp:
        if(str(i)!=' '):
            query.append(str(i))

    # print(query)
    # print(len(query))

    distinct=checkdistinct(query)
    incorrect=checkincorrect(query,int(distinct))

    if(incorrect):
        print("Incorrect SQL syntax")
        exit(-1)

    



if __name__ == "__main__":

    # store tables in dictionary
    init()

    # take command line inputs
    if(len(sys.argv)!=2):
        print("ERROR: Incorrect usage")
        print("USAGE: python3 20171091.py <sql-query>")
    else:
        try:
            q=sys.argv[1]
            
            if(len(q)==0):
                print("Incorrect SQL syntax")
                exit(-1)

            processquery(q)

        except Exception:
            traceback.print_exc()
