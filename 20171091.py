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

    for tname in tinfo:
        with open(basedr+tname+'.csv','r') as csvfile:
            csvreader=csv.reader(csvfile)

            for record in csvreader:
                ind=0
                for att in tinfo[tname]:
                    tinfo[tname][att].append(record[ind])
                    ind+=1
            
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
    
    flag=True

    if not(6>=len(query)>=4):
        flag=False

    if(query[0]!='select'):
        flag=False

    if(query[2+distinct]!='from'):
        flag=False
    
    if(4+distinct<len(query)) and (query[4+distinct][0:6]!='where '):
        flag=False

    if not flag:
        print("Incorrect SQL syntax")
        exit(-1)



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

    fields=[]#stores col name like tablename.attribute
    fields2=[]#stores col name like attribute
    aux2=[]#stores list after converting aux to a list
    aux=tinfo[tables[0]]#cumulative store of joins

    #find no of records in aux and append column header
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

    #return if only 1 table
    if(len(tables)==1):
        return [aux,fields,fields2]

    #for multiple tables
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
        a=a[6:]#remove "where "
        a=a.strip()#remove leading and trailing space and convert to list
        
        #check for and/or in condition
        oparr=[" AND "," and "," OR "," or "]
        for op in oparr:
            if(op in a):
                a=a.replace(op,',')
                a=a.split(',')
                return [a,op.split()]

        #if no and/or return cond after removing where
        return [a.split(),[]]

    #if no where clause then return empty lists
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
        if not correct:
            print("Error:operator in where clause not specified")
            exit(-1)
        
    #check if attributes specified exist or not
    for i in range(len(cond)):
        cond[i]=cond[i].replace(opused[i],',')#replace op with comma
        a=cond[i].split(',')#convert to list

        #convert str(int) to int        
        for i in range(len(a)):
            try:
                a[i]=int(a[i])
            except:
                pass

        #check in fh and fh2
        for att in a:
            if(isinstance(att,str)):
                if (fh.count(att)!=1) and (fh2.count(att)!=1):
                    print("Error: attributes not specified properly in where clause")
                    exit(-1)
            else:
                pass

    return opused



def preprocessatt(att,fh,fh2):
    """
    Converts attribute from att to tablename.att
    """

    if att not in fh:
        for i in fh:
            i=i.split(".")[0]
            i=str(i)+'.'+str(att)
            if(i in fh):
                return i
    return att



def findindex(att,fh):
    """
    Find index of att in fh
    """

    for i in range(len(fh)):
        if(att==fh[i]):
            return i



def processatt(att,fh,fh2):
    """
    Handles wether att is int or str
    """

    if(isinstance(att,str)):
        att=preprocessatt(att,fh,fh2)
        att=findindex(att,fh)
    else:
        att="-1"+str(att)
        att=int(att)

    return att



def returnindex(cond,fh,fh2):
    """
    Returns index of attributes used in cond
    """

    cond=cond.split(",")
    for i in range(len(cond)):
        try:
            cond[i]=int(cond[i])
        except:
            pass

    a1=processatt(cond[0],fh,fh2)
    a2=processatt(cond[1],fh,fh2)

    return[a1,a2]



def processnum(a):
    """
    Remove leading identifier(-1)
    """
    
    att=str(a)
    att=att[2:]
    att=int(att)
    return att



def checkexpr(a1,a2,i,opused):
    """
    Generates expression according to condition
    """

    #both are attributes
    if(a1>=0 and a2>=0):
        return str(i[a1])+str(opused)+str(i[a2])

    #a2 is number
    elif(a1>=0 and a2<0):
        a3=processnum(a2)
        return str(i[a1])+str(opused)+str(a3)
    
    #a1 is number
    elif(a1<0 and a2>=0):
        a4=processnum(a1)
        return str(a4)+str(opused)+str(i[a2])
    
    #both are numbers
    else:
        a3=processnum(a2)
        a4=processnum(a1)
        return str(a4)+str(opused)+str(a3)



def applycond(jt,fh,fh2,cond,opused,andor):
    """
    remove records form joined table if they do not meet conditions
    """
    
    #convert = to == for evaluating
    for i in range(len(opused)):
        if(opused[i]=="="):
            opused[i]="=="

    #if no where clause
    if(len(cond)==0):
        ans=[]
        for i in jt:
            ans.append(i)
        return ans
    
    #if no and/or in where clause
    elif(len(andor)==0 and len(cond)!=0):
        a1,a2=returnindex(cond[0],fh,fh2)
        ans=[]
        for i in jt:
            temp=checkexpr(a1,a2,i,opused[0])
            if eval(temp):
                ans.append(i)#if cond sattisfied then append
        return ans

    #if and/or in where clause
    elif(len(andor)==1):

        #process and/or
        andor=''.join(andor)
        andor=andor.lower()
        andor=' '+str(andor)+' '

        a1=returnindex(cond[0],fh,fh2)[0]
        a2=returnindex(cond[0],fh,fh2)[1]

        b1=returnindex(cond[1],fh,fh2)[0]
        b2=returnindex(cond[1],fh,fh2)[1]

        ans=[]
        for i in jt:
            temp=checkexpr(a1,a2,i,opused[0])
            temp2=checkexpr(b1,b2,i,opused[1])
            temp3=str(temp)+str(andor)+str(temp2)
            if eval(temp3):
                ans.append(i)
        return ans

    else:
        print("Error: Too many conditions")
        exit(-1)



def selectpreprocess(s):
    """
    Remove leading and trailing spaces from attributes
    """

    s=s.split(',')
    for i in range(len(s)):
        s[i]=s[i].strip()
    s=",".join(s)
    return s



def checkagg(att,temp,distinct):
    """
    Check if aggregate functions used or not
    """
    
    agg=[]
    aggarray=["max()","min()","sum()","avg()"]
    if(len(att)>5):
        a=att[0:4]
        a+=att[-1]
        if a in aggarray:
            #if more than 1 col selected for projection
            if(len(temp)!=1):
                print("Error: error in aggregate function")
                exit(-1)    

            #remove aggregate from att name
            att=att[4:]
            att=att[:-1]
            
            #check for distinct in att name
            att=att.strip()
            if(att[0:9]=="distinct "):
                distinct=1
                att=att[9:]
                att=att.strip()

            agg.append(a)

    return [att,agg,distinct]



def checkselect(query,fh,fh2,distinct):
    """
    Check if select statement is correct or not
    """

    if(query[1+distinct]=="*"):
        return [["*"],[],distinct]
    else:
        temp=query[1+distinct].split(",")
        for i in range(len(temp)):
            
            att=temp[i]
            att,agg,distinct=checkagg(att,temp,distinct)
            temp[i]=att

            if (fh.count(att)!=1) and (fh2.count(att)!=1):
                print("Error: attributes not specified properly in select clause")
                exit(-1)

        for i in range(len(temp)):
            temp[i]=preprocessatt(temp[i],fh,fh2)

        return [temp,agg,distinct]



def removeduplicate(ans):
    """
    Removes dupliacte columns before printing
    """

    if(len(ans)==0):
        return [[],[]]
    else:
        #transpose
        ans=list(zip(*ans))

        #get unique
        uniqans=[]
        index=[]
        for i in range(len(ans)):
            if ans[i] not in uniqans:
                uniqans.append(ans[i])
            else:
                index.append(i)

        #transpose back
        uniqans=list(zip(*uniqans))

        #convert to list
        for i in range(len(uniqans)):
            uniqans[i]=list(uniqans[i])

        return [uniqans,index]



def avg(x):
    """
    Return avg of list elements
    """
    
    return(sum(x)/len(x))



def niceprint(x,agg):
    """
    Prints a 1D list in a good way
    """

    cellwidth=10
    if(agg==1):
        cellwidth=15

    #print row seperator
    head=' '
    for i in range(len(x)):
        head+='-'*cellwidth
        head+=' '
    print(head)

    #print each record
    print('|',end='')
    for i in x:
        i=str(i)
        i=i.center(cellwidth)#ljust,rjust,center
        print(i,end='|')

    #print newline after each record
    print()



def printagg(agg,arr):
    """
    Apply aggregate functin and then print
    """

    for i in range(len(arr)):
        arr[i]=int(arr[i][0])

    exp=agg[0][:-1]
    exp+=str(arr)+")"

    niceprint([eval(exp)],1)



def printresult(fh,ans,distinct,cols,agg):
    """
    Print query result
    """
    
    if(cols==["*"]):
        
        #remove duplicate columns
        ans,index=removeduplicate(ans)
        for i in index:
            fh.pop(i)

        #print col names
        niceprint(fh,0)

        #check if distinct present
        if(distinct==1):
            uniqans=[]
            for i in ans:
                if i not in uniqans:
                    uniqans.append(i)
            for i in uniqans:
                niceprint(i,0)
        else:
            for i in ans:
                niceprint(i,0)

    else:
        #get index of attributes present in query
        index=[]
        for att in cols:
            index.append(findindex(att,fh))
        
        #print col names
        if(len(agg)==0):
            temp=[]
            for i in index:
                temp.append(fh[i])
            niceprint(temp,0)

        else:
            a=str(fh[index[0]])
            b=agg[0]
            b=b[0:4]+a+')'
            niceprint([b],1)

        #extract cols from joined table
        ans2=[]
        for record in ans:
            tr=[]
            for i in index:
                tr.append(record[i])
            ans2.append(tr)

        if(distinct==1):
            uniqans2=[]
            for i in ans2:
                if i not in uniqans2:
                    uniqans2.append(i)
            if(len(agg)==0):
                for i in uniqans2:
                    niceprint(i,0)
            else:
                printagg(agg,uniqans2)
        else:
            if(len(agg)==0):
                for i in ans2:
                    niceprint(i,0)
            else:
                printagg(agg,ans2)



def processquery(q,tinfo):
    """
    Process the given query
    """

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
    checkincorrect(query,distinct)

    #process from clause
    tables=gettables(query,distinct)
    checktables(tables)
    jt,fh,fh2 = jointable(tables,tinfo)

    #process where clause
    cond,andor=getcond(query,distinct)
    opused=checkcond(cond,fh,fh2,tables)
    ans=applycond(jt,fh,fh2,cond,opused,andor)

    #process select clause
    query[1+distinct]=selectpreprocess(query[1+distinct])
    cols,agg,distinct=checkselect(query,fh,fh2,distinct)

    #print
    printresult(fh,ans,distinct,cols,agg)
    


if __name__ == "__main__":

    #store tables in dictionary
    tinfo=init()

    #take command line inputs
    if(len(sys.argv)!=2):
        print("ERROR: Incorrect usage")
        print("USAGE: python3 20171091.py <sql-query>")
    else:
        try:
            q=sys.argv[1]
            if(len(q)==0):
                print("Error: enter a non-empty query")
                exit(-1)
            processquery(q,tinfo)
        except Exception:
            traceback.print_exc()