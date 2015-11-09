#-------------------------------------------------------------------------------
# Name:        DHSTableManagement
# Purpose:     Provides methods for joining DHS tables to generate different
#              "recodes" or outputs.
#              Inputs must be separate tables e.g. REC01, RECH1, etc.
#
# Author:      Harry Gibson
#
# Created:     09/11/2015
# Copyright:   (c) Harry Gibson 2015
# Licence:     CC BY-SA 4.0
#-------------------------------------------------------------------------------

from itertools import chain

def uniqList(input):
  output = []
  for x in input:
    if x not in output:
      output.append(x)
  return output

class TableInfo:
    """Represents information about a DB table and provides SQL to create it"""
    def __init__(self, InputTableName, JoinColumns, OutputColumns):
        self._Name = InputTableName

        # do not sort join columns as we rely on the given order to join to other tables
        self._JoinColumns = uniqList(JoinColumns)

        # create s as no-duplicates for table creation
        _uniqOut = sorted(set(OutputColumns))
        _outNoJoin = [c for c in _uniqOut if c not in JoinColumns]
        _joinToOut = [c for c in JoinColumns if c in _uniqOut]

        # output columns is whatever was provided but checked for duplicates and sorted,
        # may or may not contain join cols and if it does they are at the start
        # and not sorted
        self._OutputColumns = list(chain(_joinToOut, _outNoJoin))

        # all columns always contains the join columns in the order they were given
        # and then the other output columns in sorted order, without duplicates
        self._allColumns = list(chain(self._JoinColumns, _outNoJoin))

    def Name(self):
        """Get the string table name"""
        return self._Name

    def JoinColumns(self):
        """Get the list(string) of join column names"""
        return self._JoinColumns

    def OutputColumns(self, asString=False, qualified=False):
        """Get the columns (list or string) that should be used for output

        It is alphabetically sorted and any duplicates removed, but otherwise is the
        same as the OutputColumns provided at construction.
        This may or may not include the join columns. """
        if qualified:
            tmp = [self._Name + "." + f
                   for f in self._OutputColumns]
        else:
            tmp = self._OutputColumns
        if asString:
            return ", ".join(tmp)
        else:
            return tmp

    def AllColumns(self):
        """Get the list(string) of all columns of this table, including join columns.

        Join columns will be specified first in their original order then other columns
        in alphabetical order.
        """
        return self._allColumns

    def GetCreateTableSQL(self):
        """Get the SQL necessary to create this table in the DB. All columns are text."""
        fmt = "CREATE TABLE {0} ({1});"
        return fmt.format(self._Name,
                          ", ".join([t + " text" for t in self._allColumns]))

    def GetInsertSQLTemplate(self):
        """Get the parameterized SQL to insert rows in the DB"""
        fmt = "INSERT INTO {0} ({1}) VALUES ({2})"
        return fmt.format(self._Name,
                          ", ".join(self._allColumns),
                          ",".join(["?" for i in self._allColumns]))

    def GetCreateIndexSQL(self):
        """Get the SQL necessary to create indexes on all this table's join columns"""
        fmt = "CREATE INDEX {0} ON {1}({2});"
        idxName = "{0}_{1}"
        stmts = [fmt.format(idxName.format(jc, self._Name),
                            self._Name, jc) for jc in self._JoinColumns]
        if len(self._JoinColumns) > 0:
            allStmt = fmt.format(idxName.format("ALLIDX",self._Name),
                                 self._Name, ",".join(self._JoinColumns))
            stmts.append(allStmt)
        return "\n".join(stmts)

class TableToTableFieldCopier:
    """Provides functionality to copy fields from an input to an output table

    It provides the necessary SQL to transfer data in various different ways.
    Actually executing the SQL in a database is up to the caller.

    Both tables must already exist in the DB and be provided as TableInfo objects.
    Only the fields specified in TransferFields (and which actually exist in
    InputTable) will be copied."""
    def __init__(self, OutputTable, InputTable, TransferFields):
        self._OutputTable = OutputTable
        self._InputTable = InputTable
        self._TransferFields = [f for f in TransferFields
                        if f in InputTable.AllColumns()]

    #def _GetTransferReferences(self, tablealias):
    #    return ", ".join([tablealias + "." + f for f in self._TransferFields])

    def _GetJoinExpr(self, outCol, inCol):
        """ Returns the expression part of a join clause for one pair of table/columns

        Allows for the special case of CASEID joining to a none-caseid column
        """
        fmtSubStr = "substr({0}.{1}, 1, length({0}.{1})-3)"
        fmt = "{0} = {1}"
        expr = "{0}.{1}"
        # The CASEID variable embeds 2 ids within it. With the last three
        # characters removed (some of which may be spaces) it becomes the HHID.
        # In hindsight it would have been better to store that separately when
        # processing the raw DHS data!
        # So to join a table with CASEID to a table with HHID we need to modify
        # CASEID.
        # We could also use a query like
        # SELECT * from REC21 LEFT JOIN RECH0 ON REC21.CASEID LIKE RECH0.HHID||'%'
        # but that's very slow
        outTable = self._OutputTable.Name()
        inTable = self._InputTable.Name()
        if outCol == "CASEID" and inCol == "HHID" :
            leftExpr = fmtSubStr.format(outTable, outCol)
        else:
            leftExpr = expr.format(outTable, outCol)
        if outCol == "HHID" and inCol == "CASEID":
            rightExpr = fmtSubStr.format(inTable, inCol)
        else:
            rightExpr = expr.format(inTable, inCol)
        return fmt.format(leftExpr, rightExpr)

    def _GetJoinClause(self):
        """ Return the JOIN ON clause for joining the output to the input table.

        For example
        'outputTable.IDX1 = inputTable.IDNum AND outputTable.IDX2 = inputTable.Ref'
        The order of the join columns as provided as construction os the TableInfo
        objects is used to infer which column should be joined to which.
        """
        myJoinCols = self._OutputTable.JoinColumns()
        otherJoinCols = self._InputTable.JoinColumns()
        # If we are joining to a table with fewer join fields then only use
        # the common number of fields
        # For example the output table may specify CASEID and BIDX and can
        # join to a child table (1:M) on both of these, but can also join
        # to a parent table (1:1) on only CASEID
        # *** we assume the columns are in the matching order! ***
        if len(myJoinCols) <= len(otherJoinCols):
            otherJoinCols = otherJoinCols[0:len(myJoinCols)]
        elif len(myJoinCols) >= len(otherJoinCols):
            myJoinCols = myJoinCols[0:len(otherJoinCols)]

        joinSQL = " and ".join([self._GetJoinExpr(
                                    myJoinCols[i],
                                    otherJoinCols[i])
            for i in range (len(myJoinCols))
            ]
        )
        return joinSQL

    def _GetTransferClause(self):
        """ Returns the SET clause for copying data from input to output table.

        For example
        'outputTable.datacol1 = inputTable.datacol1'
        """
        # we don't support recording different input/output col names
        basicPart = "{0}.{1} = {2}.{1}"
        transferCols = self._TransferFields
        parts = [basicPart.format(self._OutputTable.Name(),
                         transferCols[i],
                         self._InputTable.Name(),
                         transferCols[i])
            for i in range(len(transferCols))
        ]
        return ", ".join(parts)

    def GetTransferFields(self, asString=True, qualified=False):
        """Returns the list of fields to be copied from input to output table.

        Output is either as a list or as a comma delimited string. Fields may be
        qualified with the table name, or not."""
        #GetTransferReferences(self._InputTable.Name()
        if qualified:
            tmp = [self._InputTable.Name() + "." + f
                   for f in self._TransferFields]
        else:
            tmp = self._TransferFields
        if asString:
            return ", ".join(tmp)
        else:
            return tmp

    def GetUpdateSQL_Join(self):
        """ Returns the SQL to transfer the required fields from the input to the output table.

        This version uses an inner join on the update query. This is efficient but
        SQLite doesn't support it so it is no use with SQLite db. (I found that out
        after writing it!). It may be useful in other DBs.

        For example
        UPDATE {outTbl} INNER JOIN {inTbl} ON {inTbl.id1 = outTbl.id1 AND ... }
        SET {outTblCol1 = inTblCol1, outTblCol2 = inTblCol2...};
        """
        sqlJoinTemplate = "UPDATE {0} INNER JOIN {1} ON {2} SET {3};"

        # UPDATE {0} o INNER JOIN {1} i on o.{2} = i.{3} SET o.{4} = i.{5};
        joinClause = self._GetJoinClause()
        transferClause = self._GetTransferClause()
        fields = self._GetTransferFields()
        refs = self._GetTransferReferences(self._InputTable.Name())

        outSQL = sqlJoinTemplate.format(self._OutputTable.Name(),
                           self._InputTable.Name(),
                           joinClause,
                           transferClause
                           )
        return outSQL


    def GetUpdateSQL_Replace(self):
        """ Returns the SQL to transfer the required fields from the input to the output table.

        This version uses REPLACE INTO syntax so only works for the first one on the output,
        subsequent uses will cause duplicate rows.

        For example
        'REPLACE INTO output(H0, H1, H2) SELECT i.H0, i.H1, i.H2
        FROM output o INNER JOIN REC43 i ON o.CASEID = i.CASEID;'
        """
        sqlReplaceTemplate = "REPLACE INTO {0}({1}) SELECT {2} FROM {0} INNER JOIN {3} ON {4};"

        joinClause = self._GetJoinClause()
        #transferClause = self._GetTransferClause()
        fields = self._GetTransferFields()
        #refs = self._GetTransferReferences(self._InputTable.Name())
        refs = self._GetTransferFields(qualified=True)
        outSQL = sqlReplaceTemplate.format(self._OutputTable.Name(),
                           fields,
                           refs,
                           self._InputTable.Name(),
                           joinClause
                           )
        return outSQL

    def _GetSubQuery(self, colname):
        fmt = "(SELECT {0} FROM {1} WHERE {2})"
        sql =  fmt.format(colname,
                          self._InputTable.Name(),
                          self._GetJoinClause()
        )
        return sql

    def GetUpdateSQL_SQLite(self):
        """
        Generates SQL for a join-update query that works with SQLite, using subqueries.

        All id (join) columns should be indexed else this is likely to be very slow.
        For example
        UPDATE outputTbl SET
            outCol1 = (SELECT outCol1
                      FROM inputTbl
                      WHERE idCol = outputTbl.idCol)
            , outCol2 = (SELECT outCol2
                      FROM inputTbl
                      WHERE idCol = outputTbl.idCol);
        """
        fmt = "UPDATE {0} SET {1}"
        fields = self._TransferFields
        setClause = ", ".join([col + " = " + self._GetSubQuery(col) for col in fields])
        sql = fmt.format(self._OutputTable.Name(), setClause)
        return sql


class TableToTableTransferrer(TableToTableFieldCopier):
    """ Extends TableToTableFieldCopier to also provide a method for copying rows directly.

    Used for initial population of an output table with values from a "master" table, so
    that the seeded table can then be joined to other tables.

    For example
    INSERT INTO {outTbl}({col1, col2...}) SELECT {col1, col2...} FROM {inTbl}
    """
    transferSQLTemplate = 'INSERT INTO  {0}({1}) SELECT {1} FROM {2}'

    def __init__(self, OutputTable, InputTable):
        self._OutputTable = OutputTable
        self._InputTable = InputTable
        infields = self._InputTable.AllColumns()
        self._TransferFields = infields

    def GetTransferSQL(self):

        fields = ", ".join(self._TransferFields)
        return self.transferSQLTemplate.format(self._OutputTable.Name(),
                                          fields,
                                          self._InputTable.Name()
                                          )


class MultiTableJoiner():
    """Creates an output table by joining multiple input tables together.

    The input tables are to be provided as a list of TableInfo objects.
    The first one in the list is taken as the "master" table - all rows from
    this table will be included and the other tables will all be LEFT OUTER
    joined to it. So relative to this master table all other tables must join
    either 1:1 or M:1.

    Currently all defined output columns from all the input
    tables will be used."""
    def __init__(self, OutputTableName, InputTablesList):
        self._OutputTableName = OutputTableName
        self._MasterTable = InputTablesList[0]
        self._InputTableList = InputTablesList[1:]
        self._TableCopiers = [TableToTableFieldCopier(
                                self._MasterTable, it, it.OutputColumns())
         for it in self._InputTableList
        ]

    def _GetOuterJoinClause(self, leftTable, rightTable):
        fmt = "LEFT JOIN {0} ON {1}"
        return fmt.format(leftTable, self._GetInnerJoinClause(leftTable, rightTable))

    def GetCreateIntoSQL(self):
        """Get the SQL required to create the output table from the joined inputs.

        For example:
        SELECT REC21.CASEID, REC21.COLX, REC43.COLY, REC41.COLZ from REC21
           LEFT JOIN REC43 ON REC21.CASEID = REC43.CASEID and REC21.BIDX = REC43.HIDX
           LEFT JOIN REC41 ON REC21.CASEID = REC41.CASEID and REC21.BIDX = REC41.MIDX
           ....
        """
        tblJoinTemplate = "LEFT JOIN {0} ON {1}"
        selectTemplate = "SELECT {0} from {1} {2}"
        outputTemplate = "CREATE TABLE {0} AS {1}"
        # create the left-join clause for the select statement, to
        # include all the tables we are joining
        tblJoins = " ".join([tblJoinTemplate.format(
                        i._InputTable.Name(), i._GetJoinClause())
                    for i in self._TableCopiers])
        # build the list of columns to select out of the join - it's
        # all the columns of all the joined tables, but they need to be
        # qualified where the same name is in multiple tables
        tmp = self._MasterTable.OutputColumns(qualified=True, asString=False)
        foreign = [i.GetTransferFields(qualified=True, asString=False)
                   for i in self._TableCopiers]
        flatforeign = chain.from_iterable(foreign)
        tmp.extend(flatforeign)
        tblFields = tmp#self._MasterTable.OutputColumns().extend
        #                                ([i._InputTable.OutputColumns() for i in self._TableCopiers]))
        print (len(tblFields))
        uniqFlds = ", ".join(uniqList(tblFields))#chain()
        print (len(uniqList(tblFields)))
        sel = selectTemplate.format(uniqFlds, self._MasterTable.Name(), tblJoins)
        outSQL = outputTemplate.format(self._OutputTableName, sel)
        return outSQL


