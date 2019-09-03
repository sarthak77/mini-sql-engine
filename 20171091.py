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
    return tinfo


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



def gettables(query,distinct):
    """
    Get all the tables involved in query
    """

    try:
        tables=query[3+distinct].split(",")
        for i in range(len(tables)):
            tables[i]=tables[i].strip()

        return tables        

    except:
        print("Incorrect SQL syntax")
        exit(-1)
      


def checktables(tables):
    """
    Check if tables in query are in db or not
    """

    tablelist=[]
    for i in tinfo:
        tablelist.append(i)

    for i in tables:
        if(i not in tablelist):
            print("Error: Tables not present")
            exit(-1)



def jointable(tables,tinfo):
    """
    Joins all tables which are mentioned in from
    """

    #return if only 1 table
    if(len(tables)==1):
        return tables

    # ----------------------------------
    #|preprocessing for cumulative joins|
    # ----------------------------------

    fields=[]#stores col name
    aux2=[]#stores list after converting aux to a list
    aux=tinfo[tables[0]]#cumulative store of joins

    #find no of records in aux
    noraux=0
    for k in aux:
        fields.append(str(tables[0])+'.'+str(k))
        noraux=len(aux[k])

    #covert aux to a list
    for l in range(noraux):
        row3=[]
        for m in aux.keys():
            row3.append(aux[m][l])
        aux2.append(row3)

    #copy aux2 back to aux
    aux=aux2

    #start the loop
    for i in range(1,len(tables),1):
        aux2=[]#empty aux2 after every iteration
        a=tinfo[tables[i]]#next table

        #find no of records in a
        nora=0
        for k in a:
            nora=len(a[k])
            fields.append(str(tables[i])+'.'+str(k))

        #perform join on aux and a
        for l in aux:
            for k in range(nora):
                row=[]
                for j in a.keys():
                    row.append(a[j][k])
                aux2.append(l+row)


        #copy aux2 back to aux
        aux=aux2

    return [aux,fields]


def processquery(q,tinfo):
    """
    Process the given query
    """

    # q="select max(A),table1.B,table2.C from table1,table2 where table1.A=table2.B and C=5"
    temp=sqlparse.parse(q)[0].tokens
    query=[]
    for i in temp:
        if(str(i)!=' '):
            query.append(str(i))

    # print(query)
    # print(len(query))

    distinct=checkdistinct(query)
    distinct=int(distinct)
    incorrect=checkincorrect(query,distinct)

    if(incorrect):
        print("Incorrect SQL syntax")
        exit(-1)

    # print(query)

    tables=gettables(query,distinct)
    checktables(tables)
    jt = jointable(tables,tinfo)[0]
    fh = jointable(tables,tinfo)[1]

    print(jt)
    print(fh)

    #process where



    
    


if __name__ == "__main__":

    # store tables in dictionary
    tinfo=init()

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

            processquery(q,tinfo)

        except Exception:
            traceback.print_exc()
