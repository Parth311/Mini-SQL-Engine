import sqlparse
import sys
import re

attnames = {}
attvalues = {}
columns = []
indexof = {}
col=""
distinct = False
wheref = False
gby = False
oby = False
valid = True


def metafile():
    f = open("metadata.txt", "r")
    cont = f.read()
    metalist = cont.split()
    l = len(metalist)

    for c in range(0, l):
        if metalist[c] == "<begin_table>":
            t = metalist[c+1].lower()
            attnames[t] = []
            for i in range(c+2, l):
                if metalist[i] == "<end_table>":
                    break
                columns.append(metalist[i].lower())
                attnames[t].append(metalist[i].lower())

    # print(metalist)
    #print(attnames)


def getvalues(attnames):
    for table in attnames:
        attvalues[table] = []
        f = open(table+".csv", "r")
        cont = f.read()
        val = cont.split('\n')

        for i in val:
            if i == '':
                continue
            i = i.replace('"', '')
            attvalues[table].append(i)


def getindices(att):
    global indexof
    indexof = {}
    for col in att:
        for tab in attnames:
            if col in attnames[tab]:
                if tab not in indexof:
                    indexof[tab] = []
                indexof[tab].append(attnames[tab].index(col))


def crossjoin(selectlist):
    # print(selectlist)
    cjoin = []
    temp = []

    if len(selectlist) == 1:
        return selectlist[0]
    else:
        temp = selectlist[0]
        for lst in range(1, len(selectlist)):
            for ftab in temp:
                for stab in selectlist[lst]:
                    cjoin.append(ftab+','+stab)
            temp = cjoin
            cjoin = []
        return temp


def checkgroupby(query, crossj, fetchfrom):

    if 'group by' not in query:
        return crossj

    global col
    col = query[query.index('group by')+1]
    
    print('<',end='')
    query[1]=query[1].replace(' ','')
    att = query[1].split(',')

    for i in range(0,len(att)):
        att[i]=att[i].strip()
        if i!=0:
            print(','+att[i],end='')
        else:
            print(att[i],end='')
    print('>')

    tosplit = query[1]

    agg=[]
    aggcol=[]
    for i in att:
        agg.append(i)
        if i in columns:
            tabcol = i
        #else:
        #   agg.append(i)

    #x = re.split("[\(\)]", tosplit)
    #x = x[1]
    for c in agg:
        if c not in columns:
            x = re.split("[\(\)]", c)
            aggcol.append(x[1])
    global valid
    global gby
    global distinct
    
    gby = True
    if col not in att and col not in aggcol:
        print("invalid query (operation is not performed on the right column)")
        valid = False
        return crossj

    if len(att) == 1:
        if att[0]==col:
            l=0
            distinct = True
            for tab in fetchfrom:
                if col in attnames[tab]:
                    ind1 = l+attnames[tab].index(col)
                    break
                l += len(attnames[tab])  
            tem=[]
            for row in crossj:
                lst = row.split(',')
                tem.append(int(lst[ind1]))
            crossj=tem
    else:
        temp = []
        grpdict = {}

        l = 0
        for tab in fetchfrom:
            if tabcol in attnames[tab]:
                ind1 = l+attnames[tab].index(tabcol)
                break
            l += len(attnames[tab])

        indices=[]
        for x in aggcol:
            l = 0
            for tab in fetchfrom:
                if x in attnames[tab]:
                    indices.append(l+attnames[tab].index(x))
                    break
                l += len(attnames[tab])
        ind2=indices[0]
        
        for row in crossj:
            lst = row.split(',')
            grpdict[lst[ind1]] = []
        for ind2 in indices:
        
            for row in crossj:
                lst = row.split(',')
                grpdict[lst[ind1]].append(int(lst[ind2]))

        if distinct==True:
            for key in grpdict:
                inset=set(grpdict[key])
                grpdict[key]=inset
                
        crossj = []
        if tosplit.find('sum') != -1:
            for key in grpdict:
                if len(att)!=1:
                    crossj.append(key+','+str(sum(grpdict[key])))
                else:
                    crossj.append(str(sum(grpdict[key])))
        elif tosplit.find('average') != -1 or tosplit.find('avg') != -1:
            for key in grpdict:
                if len(att)!=1:
                    crossj.append(key+','+str(sum(grpdict[key])/len(grpdict[key])))
                else:
                    crossj.append(str(sum(grpdict[key])/len(grpdict[key])))
        elif tosplit.find('min') != -1:
            for key in grpdict:
                if len(att)!=1:
                    crossj.append(key+','+str(min(grpdict[key])))
                else:
                    crossj.append(str(min(grpdict[key])))
        elif tosplit.find('max') != -1:
            for key in grpdict:
                if len(att)!=1:
                    crossj.append(key+','+str(max(grpdict[key])))
                else:
                    crossj.append(str(max(grpdict[key])))
        elif tosplit.find('count') != -1:
            for key in grpdict:
                if len(att)!=1:
                    crossj.append(key+','+str(len(grpdict[key])))
                else:
                    crossj.append(str(len(grpdict[key])))

            
    return crossj


def checkorderby(query, crossj, fetchfrom):

    if query[-3] != "order by":
        return crossj

    global oby
    global gby
    global col
    global valid
    oby = True

    ordrq = query[-2].split()
    
    coli = ordrq[0]
    
    if gby==True and coli!=col:
        print("invalid query ('group by' and 'order by' are not performed on the same column)")
        valid=False
        return
    l = 0
    ind = 0
    for tab in fetchfrom:
        if coli in attnames[tab]:
            ind = l+attnames[tab].index(coli)
            break
        l += len(attnames[tab])

    if len(ordrq)==1 or ordrq[1].lower() == "asc":
        crossj.sort(key=lambda x: int(x.split(',')[ind]))
    elif ordrq[1].lower() == "desc":
        crossj.sort(key=lambda x: int(x.split(',')[ind]), reverse=True)
    return crossj


def checkwhere(query, crossj, fetchfrom):
    new_query = []

    if query[1] != "distinct":
        new_query = query[4]
    else:
        new_query = query[5]

    if new_query.find("where") == -1:
        return crossj

    att = []
    for tab in fetchfrom:
        for c in attnames[tab]:
            att.append(c)

    andw = False
    orw = False
    global valid
    
    if new_query.find("and") == -1:
        if new_query.find("or") != -1:
            orw = True
    else:
        andw = True
    cond = []
    if andw == False and orw == False:
        cond.append(new_query[6:-1])
    elif andw == True:
        cond = (new_query[6:-1]).split(' and ')
    elif orw == True:
        cond = (new_query[6:-1]).split(' or ')

    # getindices(att)
    conlist = cond[0].split()

    for tab in indexof:
        if tab not in fetchfrom:
            print("No such columns exist")
            valid = False
            return

    cjoin = []
    attind = {}
    for col in att:
        l = 0
        for tab in fetchfrom:
            if col in attnames[tab]:
                attind[col] = l+attnames[tab].index(col)
                break
            l += len(attnames[tab])

    if len(cond) == 1:
        if conlist[1] == '=':
            conlist[1] *= 2

        for cj in crossj:
            templ = cj.split(',')
            f = attind[conlist[0]]
            if conlist[2] in att:
                s = templ[attind[conlist[2]]]
            else:
                s = conlist[2]
            c = templ[f]+conlist[1]+s

            if eval(c):
                cjoin.append(cj)

    else:
        wh1 = cond[0].split()
        wh2 = cond[1].split()

        if wh1[1] == '=':
            wh1[1] *= 2
        if wh2[1] == '=':
            wh2[1] *= 2

        for cj in crossj:
            templ = cj.split(',')
            f = attind[wh1[0]]
            s = attind[wh2[0]]

            if wh1[2] in att:
                wf = templ[attind[wh1[2]]]
            else:
                wf = wh1[2]
            if wh2[2] in att:
                ws = templ[attind[wh2[2]]]
            else:
                ws = wh2[2]

            c1 = templ[f]+wh1[1]+wf
            c2 = templ[s]+wh2[1]+ws
            if andw == True:
                if eval(c1) and eval(c2):
                    cjoin.append(cj)
            else:
                if eval(c1) or eval(c2):
                    cjoin.append(cj)

    return cjoin


def getdistinct(query, crossj):
    disval = set()
    for item in crossj:
        disval.add(item)
    crossj = []
    for t in disval:
        crossj.append(t)

    return crossj

def checkvalid(frm,whr,gb,ob):
    global valid
    if frm!=-1 and whr!=-1:
        if whr <= frm:
            print("invalid query ('where' is present before 'from')")
            valid=False
    if gb!=-1 and ob!=-1:
        if ob <= gb:
            print("invalid query ('group by' is present before 'order by')")
            valid=False


def selectall(query, notall, fetchfrom):
    global valid


    colcheck = []

    for i in fetchfrom:
        for j in attnames[i]:
            colcheck.append(j)

    selectlist = []
    getindices(colcheck)
    for table in fetchfrom:
        selectlist.append(attvalues[table])

    crossj = []
    crossj = crossjoin(selectlist)

    crossj = checkwhere(query, crossj, fetchfrom)

    crossj = checkorderby(query, crossj, fetchfrom)
    if valid == False:
        return
    crossj = checkgroupby(query, crossj, fetchfrom)
    if valid == False:
        return

    if notall == False:  # select all
        print('<', end='')
        for table in fetchfrom:
            l = len(attnames[table])
            for col in range(0, l):
                if col == l-1 and table == fetchfrom[-1]:
                    print(str(table).lower()+'.'+str(attnames[table][col]).lower(), end='>')
                else:
                    print(str(table).lower()+'.'+str(attnames[table][col]).lower(), end=',')
        print()

    elif 'group by' not in query:  # specific number of columns given
    #elif 'distinct' not in query: 
        print('<', end='')
        query[1]=query[1].replace(' ','')
        att = query[1].split(',')

        for col in att:
            if col not in colcheck:
                print("No such columns exist")
                return

        for col in att:
            for tab in fetchfrom:
                if col in attnames[tab]:
                    if col != att[-1]:
                        print(str(tab).lower()+'.'+col.lower(), end=',')
                    else:
                        print(str(tab).lower()+'.'+col.lower(), end='')
        print('>')

        getindices(att)

        skipind = {}
        l = 0
        for tab in fetchfrom:
            skipind[tab] = l
            l += len(attnames[tab])

        selectlist = []
        crossj2 = crossj
        crossj = []

        for row in crossj2:
            lis = row.split(',')
            s = ""
            for tab in indexof:
                for ind in indexof[tab]:
                    tpr = ind+skipind[tab]
                    if ind == indexof[tab][-1] and tab == list(indexof.keys())[-1]:
                        s += lis[tpr]
                    else:
                        s += lis[tpr]+','
            crossj.append(s)

    if valid == False:
        print("Invalid query")
        return

    if distinct == True:
        crossj = getdistinct(query, crossj)

    for res in crossj:
        print(res)


def performagg(query, fetchfrom):  # aggregate functions
    print('<'+query[1]+'>')

    selectlist = []
    att = []

    for tab in fetchfrom:
        selectlist.append(attvalues[tab])
    crossj = crossjoin(selectlist)

    crossj = checkwhere(query, crossj, fetchfrom)

    crossj = checkgroupby(query, crossj, fetchfrom)
    if valid == False:
        return

    crossj = checkorderby(query, crossj, fetchfrom)

    col=query[1].split(',')
    # if gby == False:
    res=""
    for t in col:
        x = re.split("[\(\)]", t)

        if query[2] == "distinct":
            crossj = getdistinct(query, crossj)
        if x[1] == '*':
            print(len(crossj))
            return

        att=[x[1]]
        getindices(att)

        tab = list(indexof.keys())[0]
        ind = list(indexof.values())[0][0]
        fetchind = 0

        for i in range(0, len(fetchfrom)):
            if fetchfrom[i] != tab:
                fetchind += len(attnames[fetchfrom[i]])
            else:
                break

        val_list = []
        if query[2] == 'distinct' and x[1] != '*':
            a = set()
            for l in crossj:
                a.add(int(l.split(',')[fetchind+ind]))
            for i in a:
                val_list.append(i)
        else:
            for l in crossj:
                val_list.append(int(l.split(',')[fetchind+ind]))
        #print(x)
        #if x[1] == '*':
        #    res+=','+str(len(crossj))
        if x[0] == 'max':
            res+=','+str(max(val_list))
        elif x[0] == 'sum':
            res+=','+str(sum(val_list))
        elif x[0] == 'min':
            res+=','+str(min(val_list))
        elif x[0] == 'average' or x[0] == 'avg':
            res+=','+str(sum(val_list)/len(val_list))
        elif x[0] == 'count':
            res+=','+str(len(val_list))
        else:
            print("Invalid Query")
            return
    
    print(res[1:])

    # if distinct == True:
     #   crossj = getdistinct(query, crossj)


def processquery(query):
    if query[-1].endswith(';')==False:
        print("invalid query (';' not present in the query)")
        return
    if 'from' not in query:
        print("invalid query ('from' not present in the query)")
        return
    else:
        if query[1] != "distinct":
            fetchfrom = query[3].split(',')
        else:
            fetchfrom = query[4].split(',')

        for i in range(0,len(fetchfrom)):
            fetchfrom[i]=fetchfrom[i].strip()

        global distinct
        global wheref
        global gby
        global oby

        for tab in fetchfrom:
            if tab not in attnames:
                print("No such table exists")
                return

        if query[1] == '*':
            notall = False

        if query[1] == "distinct":
            query[1], query[2] = query[2], query[1]
            distinct = True
            if query[1] == '*':
                notall = False
            elif (query[1].find('sum') != -1 or query[1].find('count') != -1 or query[1].find('min') != -1 or query[1].find('max') != -1 or query[1].find('average') != -1) and 'group by' not in query:
                performagg(query, fetchfrom)
                return
            else:
                notall = True

        elif query[1] != "distinct" and query[1] != "*":
            query[1]=query[1].replace(' ','')
            att = query[1].split(',')
            for i in att:
                if i not in columns and i.find('sum')==-1 and i.find('min')==-1 and i.find('max')==-1 and i.find('count')==-1 and i.find('avg')==-1 and i.find('average')==-1:
                    print("No such column exists")
                    return
                if i not in columns and 'group by' not in query:
                    notall = False
                    performagg(query, fetchfrom)
                    return
                else:
                    notall = True

        selectall(query, notall, fetchfrom)


metafile()
q = sys.argv[1]
tok = (sqlparse.parse(q)[0].tokens)
qu = sqlparse.sql.IdentifierList(tok).get_identifiers()

query = []
for i in qu:
    query.append(str(i).lower())

frmind = -1
whrind = -1
gbind = -1
obind = -1
if query[0]!="select":
    print("invalid query")
else:
    for i in range(0,len(query)):
        if query[i].find("from")!=-1:
            frmind=i
        if query[i].find("where")!=-1:
            whrind=i 
        if query[i].find("group by")!=-1:
            gbind=i
        if query[i].find("order by")!=-1:
            obind=i

    checkvalid(frmind,whrind,gbind,obind)

    if valid==True:
        getvalues(attnames)
        processquery(query)
