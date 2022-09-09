import datetime
from datetime import date
import flask
from flask import jsonify
from flask import request
from flask import redirect, url_for
import pandas as pd
from pandas.core.frame import DataFrame
import pyodbc
from pyodbc import Error
def create_connection(Driver, Server, db_name, Trsuted_Connection): # Create conneciton with sql 
    connection = None
    try:
        connection = pyodbc.connect(
            Driver=Driver,
            Server=Server, #Server name of the server when you connect to the SQL management
            database=db_name, #Database Name
            Trusted_Connection = Trsuted_Connection 
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query): # Excute code to sql 
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection, query): # Excute to read in python 
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


connection = create_connection("{SQL SERVER}","DESKTOP-0BGENB5\MSSQLSERVER02","Version2", "Yes" ) # Connects the database in sql

def AddCustomer():
    CustomerFirstName = input("Customer First Name: ")
    CustomerLastName = input("Customer Last Name: ")
    query = "SELECT * FROM Customer WHERE CustomerFirstName = '{}' and CustomerLastName = '{}'".format(CustomerFirstName,CustomerLastName)
    Result = execute_read_query(connection,query)
    if Result:
        for Customer in Result:
            CustomerID = Customer[0]
        print("In database")
    else:
        print("Not in database")
        CustomerStatusID = '1'
        State = "TX"
        query = "SELECT * FROM State WHERE StateInitial = '{}'".format(State)
        StateList = execute_read_query(connection,query)
        for State  in StateList:
            StateID = State[0]
        Country = "US"
        query = "SELECT * FROM Country WHERE CountryInitial = '{}'".format(Country)
        CountryList = execute_read_query(connection,query)
        for Country  in CountryList:
            CountryID = Country[0]
        CustomerStrike = 0
        StrikeComment = ""
        CustomerHomeNumber = input("CustomerPhoneNumber: ")
        CustomerCellNumber = input("CustomerCellNumber: ")
        query = "INSERT INTO Customer (CustomerFirstName,CustomerLastName,CustomerHomeNumber,CustomerCellNumber,StateID,CountryID,CustomerStatusID,CustomerStrike,StrikeComment) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(CustomerFirstName,CustomerLastName,CustomerHomeNumber,CustomerCellNumber,StateID,CountryID,CustomerStatusID,CustomerStrike,StrikeComment)
        execute_query(connection,query)

def AddAppointment():
    CustomerFirstName = input("Customer First Name: ")
    CustomerLastName = input("Customer Last Name: ")
    query = "SELECT * FROM Customer WHERE CustomerFirstName = '{}' and CustomerLastName = '{}'".format(CustomerFirstName,CustomerLastName)
    Result = execute_read_query(connection,query)
    if Result:
        for Customer in Result:
            CustomerID = Customer[0]
        print("In database")
        EmployeeFirstName = input("Employee First Name: ")
        EmployeeLastName = input("Employee Last Name: ")
        query = "SELECT * FROM Employee WHERE EmployeeFirstName = '{}' and EmployeeLastName = '{}'".format(EmployeeFirstName,EmployeeLastName)
        Result = execute_read_query(connection,query)
        for Employee in Result:
            EmployeeID = Employee[0]
        print(EmployeeID)
        AppointmentDate = input("Appointment Date: ")
        AppointmentTime = input("Appointment Time: ")
        query = "INSERT INTO Appointment (CustomerID,EmployeeID,AppointmentDate,AppointmentTime) VALUES ('{}','{}','{}','{}')".format(CustomerID,EmployeeID,AppointmentDate,AppointmentTime)
        execute_query(connection,query)
        query = "SELECT * FROM Appointment WHERE CustomerID = '{}' and EmployeeID = '{}' and AppointmentDate = '{}'".format(CustomerID,EmployeeID,AppointmentDate)
        AppointmentList = execute_read_query(connection,query)
        for Appointment in AppointmentList:
            AppointmentID = Appointment[0]
        ServiceName = input("Service: ")
        query = "SELECT * FROM Service WHERE ServiceName  = '{}'".format(ServiceName)
        ServiceList = execute_read_query(connection,query)
        for Service in ServiceList:
            ServiceID = Service[0]
        query = "INSERT INTO AppointmentService (AppointmentID,ServiceID) VALUES ('{}','{}')".format(AppointmentID,ServiceID)
        execute_query(connection,query)
        return 'Added Appointment'
    else:
        print("Not in database")
        CustomerStatusID = '1'
        State = "TX"
        query = "SELECT * FROM State WHERE StateInitial = '{}'".format(State)
        StateList = execute_read_query(connection,query)
        for State  in StateList:
            StateID = State[0]
        Country = "US"
        query = "SELECT * FROM Country WHERE CountryInitial = '{}'".format(Country)
        CountryList = execute_read_query(connection,query)
        for Country  in CountryList:
            CountryID = Country[0]
        CustomerStrike = 0
        StrikeComment = ""
        CustomerHomeNumber = input("CustomerPhoneNumber: ")
        CustomerCellNumber = input("CustomerCellNumber: ")
        query = "INSERT INTO Customer (CustomerFirstName,CustomerLastName,CustomerHomeNumber,CustomerCellNumber,StateID,CountryID,CustomerStatusID,CustomerStrike,StrikeComment) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(CustomerFirstName,CustomerLastName,CustomerHomeNumber,CustomerCellNumber,StateID,CountryID,CustomerStatusID,CustomerStrike,StrikeComment)
        execute_query(connection,query)
        query = "SELECT * FROM Customer WHERE CustomerFirstName = '{}' and CustomerLastName = '{}'".format(CustomerFirstName,CustomerLastName)
        CustomerList = execute_read_query(connection,query)
        for Customer in CustomerList:
            CustomerID = Customer[0]
        EmployeeFirstName = input("Employee First Name: ")
        EmployeeLastName = input("Employee Last Name: ")
        query = "SELECT * FROM Employee WHERE EmployeeFirstName = '{}' and EmployeeLastName = '{}'".format(EmployeeFirstName,EmployeeLastName)
        EmployeeList = execute_read_query(connection,query)
        for Employee in EmployeeList:
            EmployeeID = Employee[0]
        print(EmployeeID)
        AppointmentDate = input("Appointment Date: ")
        AppointmentTime = input("Appointment Time: ")
        query = "INSERT INTO Appointment (CustomerID,EmployeeID,AppointmentDate,AppointmentTime) VALUES ('{}','{}','{}','{}')".format(CustomerID,EmployeeID,AppointmentDate,AppointmentTime)
        execute_query(connection,query)
        query = "SELECT * FROM Appointment WHERE CustomerID = '{}' and EmployeeID = '{}' and AppointmentDate = '{}'".format(CustomerID,EmployeeID,AppointmentDate)
        AppointmentList = execute_read_query(connection,query)
        for Appointment in AppointmentList:
            AppointmentID = Appointment[0]
        ServiceName = input("Service: ")
        query = "SELECT * FROM Service WHERE ServiceName  = '{}'".format(ServiceName)
        ServiceList = execute_read_query(connection,query)
        for Service in ServiceList:
            ServiceID = Service[0]
        query = "INSERT INTO AppointmentService (AppointmentID,ServiceID,AppointmentDate) VALUES ('{}','{}')".format(AppointmentID,ServiceID,AppointmentDate)
        execute_query(connection,query)
        return 'Added Appointment'
       


def AddEmployee():
    EmployeeStatusID = '1'
    State = "TX"
    query = "SELECT * FROM State WHERE StateInitial = '{}'".format(State)
    StateList = execute_read_query(connection,query)
    for State  in StateList:
        StateID = State[0]
    Country = "US"
    query = "SELECT * FROM Country WHERE CountryInitial = '{}'".format(Country)
    CountryList = execute_read_query(connection,query)
    for Country  in CountryList:
        CountryID = Country[0]
    EmployeeFirstName = input("Employee First Name: ")
    EmployeeLastName = input("Employee Last Name: ")
    EmployeeRank = input("Rank: ")
    EmployeeHomeNumber = input("Employee Home Number:")
    EmployeeCellNumber = input("Employee Cell Number: ")
    EmployeeStreetName = input("Employee Street Name: ")
    EmployeeCity = input("Employee City: ")
    EmployeeZipCode = input("Employee ZipCode: ")
    HireDate = input("Hire Date: ")

    query = "SELECT * FROM EmployeeRank WHERE RankTitle = '{}'".format(EmployeeRank)
    RankList = execute_read_query(connection,query)
    for Rank in RankList:
        EmployeeRankID = Rank[0]
    query = "INSERT INTO Employee (EmployeeFirstName,EmployeeLastName,EmployeeRankID,EmployeeHomeNumber,EmployeeCellNumber,CountryID,StateID,EmployeeStreetName,EmployeeCity,EmployeeZipCode,HireDate,EmployeeStatusID) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(EmployeeFirstName,EmployeeLastName,EmployeeRankID,EmployeeHomeNumber,EmployeeCellNumber,CountryID,StateID,EmployeeStreetName,EmployeeCity,EmployeeZipCode,HireDate,EmployeeStatusID)
    execute_query(connection,query)


def AddAcquiredSkill():
    EmployeeFirstName = input("Employee First Name: ")
    EmployeeLastName = input("Employee Last Name: ")
    Skill = input("Skill: ")
    DateAcq = input("DateAcq: ")
    AcqSkillInstitution = input("AcqSkillInstitution: ")
    AcqSkillState = input("AcqState: ")
    query = "SELECT * FROM Employee WHERE EmployeeFirstName = '{}' and EmployeeLastName = '{}'".format(EmployeeFirstName,EmployeeLastName)
    EmployeeList = execute_read_query(connection,query)
    for Employee in EmployeeList:
        EmployeeID = Employee[0]
    query = "SELECT * FROM Skill WHERE SkillName = '{}'".format(Skill)
    SkillList = execute_read_query(connection,query)
    for Skill in SkillList:
        SkillID = Skill[0]
    query = "INSERT INTO AcquiredSkill (EmployeeID,SkillID,DateAcq,SkillName,AcqSkillInstitution,AcqSkillState) VALUES ('{}','{}','{}',{}','{}','{}')".format(EmployeeID,SkillID,DateAcq,Skill,AcqSkillInstitution,AcqSkillState)
    execute_query(connection,query)



def RequiredSkill():
    ServiceName = input("Service: ")
    SkillName = input("Skill: ")
    query = "SELECT * FROM Service WHERE ServiceName = '{}'".format(ServiceName)
    ServiceList = execute_read_query(connection,query)
    for Service in ServiceList:
        ServiceID = Service[0]
    query = "SELECT * FROM Skill WHERE SkillName = '{}'".format(SkillName)
    SkillList = execute_read_query(connection,query)
    for Skill in SkillList:
        SkillID = Skill[0]
    query = "INSERT INTO RequiredSkill (ServiceID,SkillID,ReqSkillName,ServiceName) VALUES ('{}','{}'.'{}','{}')".format(ServiceID,SkillID,SkillName,ServiceName)
    execute_query(connection,query)



def AddSatisfactionRating():
    CustomerFirstName = input("Customer First Name: ")
    CustomerLastName = input("Customer Last Name: ")
    AppointmentDate = input("Date: ")
    query = "SELECT * FROM Customer WHERE CustomerFirstName = '{}' and CustomerLastName = '{}'".format(CustomerFirstName,CustomerLastName)
    Result = execute_read_query(connection,query)
    for Customer in Result:
        CustomerID = Customer[0]
    query = "SELECT * FROM Appointment WHERE CustomerID = '{}'and AppointmentDate = '{}'".format(CustomerID,AppointmentDate)
    AppointmentList = execute_read_query(connection,query)
    for Appointment in AppointmentList:
        AppointmentID = Appointment[0]
        EmployeeID = Appointment[2]
    AppointmentSatisfaction = input("Satisfaction: ")
    Comment = input("Test")
    query = "INSERT INTO SatisfactionRating (EmployeeID,AppointmentID,SatisfactionMeaningID,AppointmentSatisfaction,Comments) VALUES ('{}','{}','{}','{}','{}')".format(EmployeeID,AppointmentID,AppointmentSatisfaction,AppointmentSatisfaction,Comment)
    execute_query(connection,query)

def AddEmployeeRole():
    EmployeeFirstName = input("Employee First Name: ")
    EmployeeLastName = input("Employee Last Name: ")
    RoleTitle = input("Role: ")
    query = "SELECT * FROM Employee WHERE EmployeeFirstName = '{}' and EmployeeLastName = '{}'".format(EmployeeFirstName,EmployeeLastName)
    EmployeeList = execute_read_query(connection,query)
    for Employee in EmployeeList:
        EmployeeID = Employee[0]
    query = "SELECT * FROM Role WHERE RoleTitle = '{}'".format(RoleTitle)
    RoleList = execute_read_query(connection,query)
    for Role in RoleList:
        RoleID = Role[0]
    YearofRole = input("Year: ")
    query = "INSERT INTO EmployeeRole (EmployeeID,RoleID,YearofRole) VALUES ('{}','{}','{}')".format(EmployeeID,RoleID,YearofRole)
    execute_query(connection,query)

def AddEmployeeSchedule():
    EmployeeFirstName = input("Employee First Name: ")
    EmployeeLastName = input("Employee Last Name: ")
    ClockIn = input("ClockIn: ")
    ClockOut = input("ClockOut: ")
    DayofWeek = input("Date: ")
    Day = datetime.datetime.strptime(DayofWeek, "%m/%d/%Y")
    Day = Day.strftime('%A')
    query = "SELECT * FROM Employee WHERE EmployeeFirstName = '{}' and EmployeeLastName = '{}'".format(EmployeeFirstName,EmployeeLastName)
    EmployeeList = execute_read_query(connection,query)
    for Employee in EmployeeList:
        EmployeeID = Employee[0]
    query = "SELECT * FROM DaysofOperation WHERE DayName = '{}'".format(Day)
    DayList = execute_read_query(connection,query)
    for DaysofOperation in DayList:
        DaysofOperationID = DaysofOperation[0]
    query = "INSERT INTO EmployeeSchedule (EmployeeID,DaysofOperationID,EmployeeFirstName,EmployeeLastName,DayofWeek,ClockIn,ClockOut) VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(EmployeeID,DaysofOperationID,EmployeeFirstName,EmployeeLastName,DayofWeek,ClockIn,ClockOut)
    execute_query(connection,query)


AddEmployeeSchedule()