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
        return [tables,[],[]]

    # ----------------------------------
    #|preprocessing for cumulative joins|
    # ----------------------------------

    fields=[]#stores col name like table1.A
    fields2=[]#stores col name like A
    aux2=[]#stores list after converting aux to a list
    aux=tinfo[tables[0]]#cumulative store of joins

    #find no of records in aux
    noraux=0
    for k in aux:
        fields.append(str(tables[0])+'.'+str(k))
        fields2.append(str(k))
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
            fields2.append(str(k))

        #perform join on aux and a
        for l in aux:
            for k in range(nora):
                row=[]
                for j in a.keys():
                    row.append(a[j][k])
                aux2.append(l+row)


        #copy aux2 back to aux
        aux=aux2

    return [aux,fields,fields2]



def getcond(query,distinct):
    """
    Get all the conditions specified in where clause
    """

    if(4+distinct<len(query)) and (query[4+distinct][0:6]=='where '):

        a=query[4+distinct]
        a=a[6:]
        a=a.strip()
        
        #check for and/or
        oparr=[" AND "," and "," OR "," or "]
        for op in oparr:
            if(op in a):
                a=a.replace(op,',')
                a=a.split(',')
                # print(a)
                return [a,op.split()]

        #if no and/or return cond after removing where
        return [a.split(),[]]

    return [[],[]]



def checkcond(cond,fh,fh2,tables):
    """
    Check if conditions specified in where clause are correct or not
    """

    oparr=["<=",">=","<",">","="]
    opused=[]#stores op used in each cond

    #check for condition syntax
    for i in cond:

        correct=False
        #if any one op present in cond then correct
        for j in oparr:
            if(j in i):
                correct=True
                opused.append(j)
                break
        if(not correct):
            print("Error:operator in where clause not specified")
            exit(-1)
        
    #check if attributes specified exist or not
    for i in range(len(cond)):
        cond[i]=cond[i].replace(opused[i],',')
        a=cond[i].split(',')

        #check in fh and fh2
        for att in a:
            if (fh.count(att)!=1) and (fh2.count(att)!=1):
                print("Error: attributes not specified properly in where clause")
                exit(-1)

    return opused



def processatt(att,fh,fh2):
    if att not in fh:
        for i in fh:
            i=i.split(".")[0]
            i=str(i)+'.'+str(att)
            if(i in fh):
                return i

    return att



def findindex(att,fh):
    for i in range(len(fh)):
        if(att==fh[i]):
            return i



def returnindex(cond,fh,fh2):

        cond=cond.split(",")

        a1=cond[0]
        a2=cond[1]
        
        a1=processatt(a1,fh,fh2)
        a2=processatt(a2,fh,fh2)
        # print(a1,a2)
        
        a1=findindex(a1,fh)
        a2=findindex(a2,fh)
        # print(fh)
        # print(a1,a2)
        return[a1,a2]



def applycond(jt,fh,fh2,cond,opused,andor):
    """
    remove records form joined table if they do not meet conditions
    """
    
    if(len(andor)==0):
        a1=returnindex(cond[0],fh,fh2)[0]
        a2=returnindex(cond[0],fh,fh2)[1]

        ans=[]
        for i in jt:
            temp=str(i[a1])+str(opused[0])+str(i[a2])
            if eval(temp):
                ans.append(i)
        return ans

    elif(len(andor)==1):

        andor=''.join(andor)
        andor=andor.lower()
        andor=' '+str(andor)+' '

        a1=returnindex(cond[0],fh,fh2)[0]
        a2=returnindex(cond[0],fh,fh2)[1]

        b1=returnindex(cond[1],fh,fh2)[0]
        b2=returnindex(cond[1],fh,fh2)[1]

        ans=[]
        for i in jt:
            temp=str(i[a1])+str(opused[0])+str(i[a2])
            temp2=str(i[b1])+str(opused[1])+str(i[b2])
            temp3=str(temp)+str(andor)+str(temp2)
            # print(temp3)

            if eval(temp3):
                ans.append(i)
        return ans

    else:
        print("Error: Too many conditions")
        exit(-1)



def checkselect(query,fh,fh2):

    if(query[1]=="*"):
        return ["*"]

    else:
        temp=query[1].split(",")

        for att in temp:
            if (fh.count(att)!=1) and (fh2.count(att)!=1):
                print("Error: attributes not specified properly in select clause")
                exit(-1)

        for i in range(len(temp)):
            temp[i]=processatt(temp[i],fh,fh2)

        # print(temp)
        return temp



def printresult(fh,ans,distinct,cols):

    if(distinct==1):
        ans=list(dict.fromkeys(ans))

    if(cols==["*"]):
        print(fh)
        for i in ans:
            print(i)
    else:
        index=[]
        for att in cols:
            index.append(findindex(att,fh))

        for i in index:
            print(fh[i],end=' ')
        print()

        for record in ans:
            for i in index:
                print(record[i],end=' ')
            print()
        


def processquery(q,tinfo):
    """
    Process the given query
    """

    # q="select max(A),table1.B,table2.C from table1,table2 where table1.A=table2.B and C=5"
  
    #convert query to a list
    temp=sqlparse.parse(q)[0].tokens
    query=[]
    for i in temp:
        if(str(i)!=' '):
            query.append(str(i))

    #check if distinct present
    distinct=checkdistinct(query)
    distinct=int(distinct)

    #check if incorrect
    incorrect=checkincorrect(query,distinct)
    if(incorrect):
        print("Incorrect SQL syntax")
        exit(-1)

    #process from clause
    tables=gettables(query,distinct)
    checktables(tables)
    jt,fh,fh2 = jointable(tables,tinfo)

    #process where clause
    cond,andor=getcond(query,distinct)
    opused=checkcond(cond,fh,fh2,tables)
    ans=applycond(jt,fh,fh2,cond,opused,andor)

    #process select clause
    cols=checkselect(query,fh,fh2)
    
    #print
    printresult(fh,ans,distinct,cols)
    


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
