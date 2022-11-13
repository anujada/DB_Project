

import psycopg2  #import of the psycopg2 python library
import pandas as pd #import of the pandas python library
import pandas.io.sql as psql



##No transaction is started when commands are executed and no commit() or rollback() is required. 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


try:
    # Connect to the postgreSQL server with username, and password credentials
    con = psycopg2.connect(user = "postgres",
                                  host = "localhost",
                                  port = "5432")
    
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
    print("Connected Successfully to PostgreSQL server!!")
    
    # Obtain a DB Cursor to perform database operations
    cursor = con.cursor();
except (Exception, psycopg2.Error) as error :
     print ("Error while connecting to PostgreSQL", error)

# create teams


try:
    #table_name variable
    teamsTable="teams"
    create_teamsTable_query = '''CREATE TABLE IF NOT EXISTS '''+ teamsTable+''' 
              (team_id INT  PRIMARY KEY     NOT NULL,
               team_name     TEXT    NOT NULL,
               team_shortname        TEXT  NOT NULL ,
               team_nationality         TEXT  NOT NULL ,
               team_powerunit TEXT NOT NULL    , 
               team_principle TEXT  NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_teamsTable_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ teamsTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)


### insert values in teams

sql_insert_teams = "INSERT INTO teams (team_id, team_name, team_shortname, team_nationality, team_powerunit, team_principle) VALUES(%s,%s,%s,%s,%s, %s)"

teams_List=[(1,	"Alfa Romeo",	"Alfa Romeo",	"Switzerland",	"Ferrari",	"Frédéric Vasseur"),
            (2,	"Alpha Tauri",	"AlphaTauri", "Italy",	"Red Bull Powertrains",	"Franz Tost"),
            (3,	"BWT Alpine F1 Team",	"Alpine", "France",	"Renault", 	"Otmar Szafnauer"),
            (4,	"Aston Martin Aramco Cognizant F1 Team",	"Aston Martin",	"United Kingdom",	"Mercedes",	"Mike Krack"),
            (5,	"Scuderia Ferrari",	"Ferrari",	"Italy",	"Ferrari",	"Mattia Binotto"),
            (6,	"Haas F1 Team",	"Haas",	"United States",	"Ferrari",	"Guenther Steiner"),
            (7,	"McLaren F1 Team",	"McLaren",	"United Kingdom",	"Mercedes",	"Andreas Seidl"),
            (8,	"Mercedes-AMG Petronas F1 Team",	"Mercedes",	"Germany",	"Mercedes",	"Toto Wolff"),
            (9,	"Oracle Red Bull Racing",	"Red Bull",	"Austria",	"Red Bull Powertrains",	"Christian Horner"),
            (10,	"Williams Racing",	"Williams",	"United Kingdom",	"Mercedes",	"Jost Capito")]

try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_teams,teams_List)
    # commit the changes to the database
    con.commit()
    #the number of inserted rows/tuples
    count = cursor.rowcount
    print (count, "Record inserted successfully into teams table")

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while Inserting the data to the table, Details: ",error)


# create Drivers

try:
    driversTable="drivers"
    create_driversTable_query = '''CREATE TABLE IF NOT EXISTS "'''+ driversTable+'''" 
              (driver_id INT  PRIMARY KEY   NOT NULL,
               team_id  INT  REFERENCES teams(team_id)  NOT NULL,
               driver_code varchar(3) NOT NULL, 
               driver_number    INT    NOT NULL,
               driver_name TEXT NOT NULL, 
               driver_nationality TEXT, 
               driver_age INT
               ); '''
    
    #Execute this command (SQL Query)
    cursor.execute(create_driversTable_query)
    con.commit()
    print("Table ("+ driversTable +") created successfully in PostgreSQL ")
except:
    con.rollback()
    print("Table ("+ driversTable +") already Existed! ")


# Insert data in drivers

sql_insert_drivers = '''INSERT INTO drivers (driver_id, team_id, driver_code, driver_number, driver_name, driver_nationality, driver_age ) VALUES(%s,%s,%s,%s,%s,%s,%s)'''

#list of customers
drivers_List=[
                (1, 9,	"VER",	1,	"Max Verstappen",	"The Netherlands",	25),
                (3, 7,	"RIC",	3,	"Daniel Ricciardo",	"Australia",	33),
                (4, 7,	"NOR",	4,	"Lando Norris",	"United Kingdom",	23),
                (5, 4,	"VET",	5,	"Sebastian Vettel",	"Germany",	35),
                (6, 10,	"LAT",	6,	"Nicholas Latifi",	"Canada",	27),
                (10, 2,"GAS",	10,	"Pierre Gasly",	"France",	26),
                (11, 9, "PER",	11,	"Sergio Perez",	"Messico",	32),
                (14, 3, "ALO",	14,	"Fernando Alonso",	"Spain"	,41),
                (16,5, "LEC",	16,	"Charles Leclerc",	"Monaco",	25),
                (18, 4, "STR",	18,	"Lance Stroll",	"Canada",	24),
                (20,6, "MAG",	20,	"Kevin Magnusse",	"Denmark",	30),
                (22,2,"TSU",	22,	"Yuki Tsunoda",	"Japan",	22),
                (23, 10, "ALB",	23,	"Alexander Albon",	"Thailand",	26),
                (24,1, "ZHO",	24,	"Zhou Guanyu"	,"China",	23),
                (31,3, "OCO",	31,	"Estaban Ocon",	"France",	26),
                (44	,8, "HAM",	44,	"Lewis Hamilton",	"United Kingdom",	37),
                (47,6, "SCH",	47,	"Mick Schumacher",	"Germany",	23),
                (55,5,"SAI",	55,	"Carlos Sainz",	"Spain",	28),
                (63,8,"RUS",	63,	"George Russell",	"United Kingdom",	24),
                (77,1,"BOT",	77,	"Valtteri Bottas" , "Finland",	33),
                (19,10,"VRI",	88,	"Nick De Vries",	"The Netherlands",	27),
                (27, 4, "HUL",	99,	"Nico Hulkenberg",	"Germany",	35),]
try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_drivers,drivers_List)
    # commit the changes to the database
    con.commit()

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while exccuting the query at PostgreSQL",error)
    
finally:
    
    count = cursor.rowcount
    print (count, "Record inserted successfully into drivers table")


# create finalStatus 


try:
    finalStatusTable="finalstatus"
    create_finalStatusTable_query = '''CREATE TABLE IF NOT EXISTS"'''+ finalStatusTable+'''" 
              (finalstatus_id INT  PRIMARY KEY NOT NULL,
               finalstatus_description  TEXT
               ); '''

    
    #Execute this command (SQL Query)
    cursor.execute(create_finalStatusTable_query)
    con.commit()
    print("Table ("+ finalStatusTable +") created successfully in PostgreSQL ")
except:
    con.rollback()
    print("Table ("+ finalStatusTable +") already Existed! ")


    # Insert data into finalStatus 

    sql_insert_finalstatus = '''INSERT INTO finalstatus (finalstatus_id,finalstatus_description) VALUES(%s,%s)'''

    #list of customers
    finalstatus_List=[
                (1,	"Finished"),
                (2,	"Disqualified"),
                (3,	"Accident"),
                (4,	"Collision"),
                (5,	"Engine"),
                (6,	"Gearbox"),
                (7,	"Transmission"),
                (8,	"Clutch"),
                (9,	"Hydraulics"),
                (10,	"Electrical"),
                (11,	"+1 Lap"),
                (12,	"+2 Laps"),
                (13,	"+3 Laps"),
                (14,	"+4 Laps"),
                (15,	"+5 Laps"),
                (16,	"+6 Laps"),
                (17,	"+7 Laps"),
                (18,	"+8 Laps"),
                (19,	"+9 Laps"),
                (20,	"Spun off"),
                (21,	"Radiator"),
                (22,	"Suspension"),
                (23,	"Brakes"),
                (24,	"Differential"),
                (25,	"Overheating"),
                (26,	"Mechanical"),
                (27,	"Tyre"),
                (28,	"Driver Seat"),
                (29,	"Puncture"),
                (30,	"Driveshaft"),
                (31,	"Retired"),
                (32,	"Fuel pressure"),
                (33,	"Front wing"),
                (34,	"Water pressure"),
                (35,	"Refuelling"),
                (36,	"Wheel"),
                (37,	"Throttle"),
                (38,	"Steering"),
                (39,	"Technical"),
                (40,	"Electronics"),
                (41,	"Broken wing"),
                (42,	"Heat shield fire"),
                (43,	"Exhaust"),
                (44,	"Oil leak"),
                (45,	"+11 Laps"),
                (46,	"Wheel rim"),
                (47,	"Water leak"),
                (48,	"Fuel pump"),
                (49,	"Track rod"),
                (50,	"+17 Laps"),
                (51,	"Oil pressure"),
                (53,	"+13 Laps"),
                (54,	"Withdrew"),
                (55,	"+12 Laps"),
                (56,	"Engine fire"),
                (58,	"+26 Laps"),
                (59,	"Tyre puncture"),
                (60,	"Out of fuel"),
                (61,	"Wheel nut"),
                (62,	"Not classified"),
                (63,	"Pneumatics"),
                (64,	"Handling"),
                (65,	"Rear wing"),
                (66,	"Fire"),
                (67,	"Wheel bearing"),
                (68,	"Physical"),
                (69,	"Fuel system"),
                (70,	"Oil line"),
                (71,	"Fuel rig"),
                (72,	"Launch control"),
                (73,	"Injured"),
                (74,	"Fuel"),
                (75,	"Power loss"),
                (76,	"Vibrations"),
                (77,	"107% Rule"),
                (78,	"Safety"),
                (79,	"Drivetrain"),
                (80,	"Ignition"),
                (81,	"Did not qualify"),
                (82,	"Injury"),
                (83,	"Chassis"),
                (84,	"Battery"),
                (85,	"Stalled"),
                (86,	"Halfshaft"),
                (87,	"Crankshaft"),
                (88,	"+10 Laps"),
                (89,	"Safety concerns"),
                (90,	"Not restarted"),
                (91,	"Alternator"),
                (92,	"Underweight"),
                (93,	"Safety belt"),
                (94,	"Oil pump"),
                (95,	"Fuel leak"),
                (96,	"Excluded"),
                (97,	"Did not prequalify"),
                (98,	"Injection"),
                (99,	"Distributor"),
                (100,	"Driver unwell"),
                (101,   "Turbo"),
                (102,   "CV joint"),
                (103,	"Water pump"),
                (104,	"Fatal accident"),
                (105,	"Spark plugs"),
                (106,	"Fuel pipe"),
                (107,	"Eye injury"),
                (108,	"Oil pipe"),
                (109,	"Axle"),
                (110,	"Water pipe"),
                (111,	"+14 Laps"),
                (112,	"+15 Laps"),
                (113,	"+25 Laps"),
                (114,	"+18 Laps"),
                (115,	"+22 Laps"),
                (116, "+16 Laps"),
                (117,	"+24 Laps"),
                (118,	"+29 Laps"),
                (119,	"+23 Laps"),
                (120,	"+21 Laps"),
                (121,"	Magneto"),
                (122,	"+44 Laps"),
                (123,	"+30 Laps"),
                (124,	"+19 Laps"),
                (125,	"+46 Laps"),
                (126,	"Supercharger"),
                (127,	"+20 Laps"),
                (128,	"+42 Laps"),
                (129,	"Engine misfire"),
                (130,	"Collision damage"),
                (131,	"Power Unit"),
                (132,	"ERS"),
                (133,	"+49 Laps"),
                (134,	"+38 Laps"),
                (135,	"Brake duct"),
                (136,	"Seat"),
                (137,	"Damage"),
                (138,	"Debris"),
                (139,	"Illness"),
                (140, "Undertray"),
                (141,	"Cooling system)")]
    try:
        # execute the INSERT statement
        cursor.executemany(sql_insert_finalstatus,finalstatus_List)
        # commit the changes to the database
        con.commit()

    except (Exception, psycopg2.Error) as error :
        con.rollback()
        print ("Error while exccuting the query at PostgreSQL",error)
        
    finally:
        
        count = cursor.rowcount
        print (count, "Record inserted successfully into finalStatus table")


#create granPrix table

try:
    #table_name variable
    granprixTable="granprix"
    create_granprixTable_query = '''CREATE TABLE '''+ granprixTable+''' 
              (granprix_id INT  PRIMARY KEY     NOT NULL,
               granprix_name        TEXT    NOT NULL,
               granprix_circuit    TEXT    NOT NULL,
               granprix_location TEXT ,
               granprix_length   INT ,  
               granprix_orderScheduled INT , 
               granprix_dataScheduled TEXT ,
               granprix_lapsScheduled INT ,
               granprix_totalLength FLOAT(20),
               granprix_sponsor TEXT
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_granprixTable_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ granprixTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

# Insert data in granPrix

sql_insert_granprix = '''INSERT INTO "granprix" (granprix_id, granprix_name, granprix_circuit, granprix_location, granprix_length, granprix_orderScheduled, granprix_dataScheduled	,granprix_lapsScheduled	, granprix_totalLength ,granprix_sponsor) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

#list of customers
granprix_List=[
(1,	"Bahrain",	"Bahrain International Circuit",	"Bahrain",	5412,	1,	"2022-03-20", 	57,	308.238,	"Gulf Air"),
(2,	"Saudia Arabia",	"Jeddah Corniche Circuit",	"Saudia Arabia",	6174,	2,	"2022-03-27", 50,	308.450,	"Heineken"),
(3,	"Australia",	"Albert Park Circuit",	"Australia",	5279,	3,	"2022-04-10",	58,	306.240,	"Heineken"),
(4,	"Emilia Romagna",	"Autodromo Internazionale Enzo e Dino Ferrari",	"Italy",	4909,	4,	"2022-04-24", 63,	309.049,	"Pirelli"),
(5,	"Miaimi",	"Miami International Autodrome",	"United States",	5410,	5,	"2022-05-08",	57,	308.370,	"cripto.com"),
(6,	"Spain",	"Circuit de Barcelona-Catalunya",	"Spain",	4675,	6,	"2022-05-22",	66,	308.424,	"Santader"),
(7,	"Monaco",	"Circuit de Monaco",	"Monaco",	3337,	7,	"2022-05-29",	78,	260.286,	"TAG Heuer"),
(8,	"Azerbaijan",	"Baku City Circuit",	"Azerbaijan",	6003,	8,	"2022-06-12",	51,	306.049,	"Bakcell"),
(9,	"Canada",	"Circuit Gilles Villeneuve",	"Canada",	4361,	9,	"2022-06-19",70,	305.270	, "AWS"),
(10,	"Great Britain",	"Silverstone Circuit",	"Great Britain",	5891,	10,	"2022-07-03",	52,	306.198	,"Bremont"),
(11,	"Austria",	"Red Bull Ring",	"Austria",	4318,	11,	"2022-07-10",	71,	306.452,	"Oracle"),
(12,	"France	","Circuit Paul Ricard",	"France",	5842,	12,	"2022-07-24",	53,	309.690,	"Lenovo"),
(13,	"Hungary",	"Hungaroring",	"Hungary",	4381,	13,	"2022-07-31",	70,	306.630,	"Heineken"),
(14,	"Belgium",	"Circuit de Spa-Francorchamps",	"Belgium",	7004,	14,	"2022-08-28",	44,	308.052,	"Rolex"),
(15,	"Netherlands",	"Circuit Zandvoort",	"Netherlands",	4259,	15,	"2022-09-04",	72,	306.587,	"Heineken"),
(16,	"Italy",	"Autodromo Nazionale di Monza",	"Italy",	5793,	16,	"2022-09-11",	53,	306.720	,"Pirelli"),
(17,	"Singapore",	"Marina Bay Street Circuit",	"Singapore",	5063,	17,	"2022-10-02",	61,	308.706,	"Heineken"),
(18,	"Japan",	"Suzuka Circuit",	"Japan",	5807,	18,	"2022-10-09",	53,	307.471,	"Heineken"),
(19,	"United States",	"Circuit of the Americas",	"United States",	5513,	19,	"2022-10-23",	56,	308.405,	"Aramco"),
(20,	"Mexico","Autódromo Hermanos Rodríguez",	"Mexico", 304,	20,	"2022-10-30",	71,	305.354	,"IVO"),
(21,	"Brazil", "Autódromo José Carlos Pace",	"Brazil",	4309,	21,	"2022-11-13",	71,	305.879,	"Heineken"),
(22,	"Abu Dhabi",	"Yas Marina Circuit",	"Abu Dhabi",	5281,	22,	"2022-11-2",	58,	306.183,	"Etihad Airways")]

try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_granprix, granprix_List)
    # commit the changes to the database
    con.commit()

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while exccuting the query at PostgreSQL",error)
    
finally:
    
    count = cursor.rowcount
    print (count, "Record inserted successfully into granprix table")



#Create Results Table 

try:
    resultsTable="results"
    create_resultsTable_query = '''CREATE TABLE"'''+ resultsTable+'''" 
              (result_id INT  PRIMARY KEY NOT NUlL,
               granprix_id  INT  REFERENCES granprix(granPrix_id) NOT NULL,
               driver_id  INT  REFERENCES drivers(driver_id) NOT NULL,
               result_gridposition INT, 
               result_finalposition INT, 
               result_lapscompleted INT, 
               result_driverpoints INT,
               result_fastestlap INT,
               result_fastestlaptime TIME, 
               result_fastestlaprank INT, 
               result_fastestlapspeed FLOAT(20),
               status_id INT REFERENCES "finalstatus"(finalstatus_id)
               ); '''

    
   #Execute this command (SQL Query)
    cursor.execute(create_resultsTable_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ resultsTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)



# Insert data into results
sql_insert_results = '''INSERT INTO "results" (result_id,granprix_id,driver_id,result_gridposition,result_finalposition,result_lapscompleted,result_driverpoints,result_fastestlap,result_fastestlaptime,result_fastestlaprank,result_fastestlapspeed,status_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

results_List=[     
        (1,	1,	16,	1,	1,	57,	26,	51,	'1:34.570',	1,	206.018,	1),
        (2,	1,	55,	3,	2,	57,	18,		52,	'1:35.740',	3,	203.501,	1),
        (3,	1,	44,	5,	3,	57,	15,		53,	'1:36.228',	5,	202.469,	1),
        (4,	1,	63,	9,	4,	57,	12,		56,	'1:36.302',	6,	202.313,	1),
        (5,	1,	20,	7,	5,	57,	10,		53,	'1:36.623',	8,	201.641,	1),
        (6,	1,	77,	6,	6,	57,	8,		53,	'1:36.599',	7,	201.691,	1),
        (7,	1,	31,	11,	7,	57,	6,		53,	'1:37.110',	14,	200.630,	1),
        (8,	1,	22,	16,	8,	57,	4,		53,	'1:37.104',	13,	200.642,	1),
        (9,	1,	14,	8,	9,	57,	2,		44,	'1:36.733',	10,	201.412,	1),
        (10,	1,	24,	15,	10,	57,	1,		39,	'1:36.685',	9	,201.512,   1),
        (11,	1,	47,	12,	11,	57,	0,		37,	'1:36.956',	11,	200.948,	1),
        (12,	1,	18,	19,	12,	57,	0,		49,	'1:37.146',	15,	200.555,	1),
        (13,	1,	23,	14,	13,	57,	0,		50,	'1:37.355',	18,	200.125,	1),
        (14,	1,	3,	18,	14,	57,	0,		50,	'1:37.261',	16,	200.318,	1),
        (15,	1,	4,	13,	15,	57,	0,		51,	'1:36.988',	12,	200.882,	1),
        (16,	1,	6	,20	,16	,57	,0,	51,	'1:38.251',	20,	198.300,	1),
        (17,	1,	27,	17,	17,	57,	0,	49,	'1:38.201',	19,	198.401,	1),
        (18,	1,	11,	4,	18,	56,	0,	52,	'1:36.089',	4,	202.762,	32),
        (19,	1,	1,	2,	19,	54,	0,		51,	'1:35.440',	2,	204.140,	32),
        (20,	1,	10,	10,	20,	44,	0,		34,	'1:37.324',	17	,200.189,	131),
        (21,	2,	1,	4,	1,	50,	25,		50,	'1:31.772',	2,	242.191,	1),
        (22,	2,	16,	2,	2,	50,	19,		48,	'1:31.634',	1,	242.556,	1),
        (23,	2,	55,	3,	3,	50,	15,	48,	'1:31.905',	3,	241.841,	1),
        (24,	2,	11,	1,	4,	50,	12,		46,	'1:32.042',	4,	241.481,	1),
        (25,	2,	63,	6,	5,	50,	10,		43,	'1:32.821',	7,	239.454,	1),
        (26,	2,	31,	5,	6,	50,	8,		46,	'1:33.103',	9,	238.729,	1),
        (27,	2,	4,	11,	7,	50,	6,		46,	'1:32.753',	5,	239.629,	1),
        (28,	2,	10,	9,	8,	50,	4,		42,	'1:33.468',	10,	237.796,	1),
        (29,	2,	20,	10,	9,	50,	2,		48,	'1:32.779',	6,	239.562,	1),
        (30,	2,	44,	15,	10,	50,	1,		47,	'1:32.997',	8,	239.001,	1),
        (31,	2,	24,	12,	11,	50,	0,		45,	'1:33.924',	13,	236.642,	1),
        (32,	2,	27,	17,	12,	50,	0,		47,	'1:33.651',	11,	237.332,	1),
        (33,	2,	18,	13,	13,	49,	0,	46,	'1:34.446',	16,	235.334,	11),
        (34,	2,	23,	16,	14,	47,	0,	46,	'1:34.368',	15,	235.528,	130),
        (35,	2,	77,	8,	15,	36,	0,	31,	'1:33.979',	14,	236.503,	141),
        (36,	2,	14,	7,	16,	35,	0,	33,	'1:33.831',	12,	236.876,	103),
        (37,	2,	3,	14,	17,	35,	0,	34,	'1:34.487',	17,	235.232,	6),
        (38,	2,	6,	18,	18,	14,	0,		9	,'1:37.530',	18,	227.892,	3),
        (39,	2,	22,	19,	19,	0,	0,		None,	None,	0,	None,	131),
        (40,	2,	47,	0, 20,	0,	0,		None,	None,	0,	None,	54),
        (41,	3,	16,	1,	1,	58,	26,		58,	'1:20.260',	1,	236.740,	1),
        (42,	3,	11,	3,	2,	58,	18,		58,	'1:21.094',	3,	234.305,	1),
        (43,	3,	63,	6,	3,	58,	15,		53,	'1:21.495',	4,	233.152,	1),
        (44,	3,	44,	5,	4,	58,	12,		51,	'1:21.886',	7,	232.039,	1),
        (45,	3,	4,	4,	5,	58,	10,		55,	'1:22.248',	8,	231.018,	1),
        (46,	3,	3,	7,	6,	58,	8,		54,	'1:22.451',	9,	230.449,	1),
        (47,	3,	31,	8,	7,	58,	6,		58,	'1:22.469',	10,	230.399,	1),
        (48,	3,	77,	12,	8,	58,	4,		54,	'1:21.651',	5,	232.707,	1),
        (49,	3,	10,	11,	9,	58,	2,		52,	'1:22.731',	13,	229.669,	1),
        (50,	3,	23,	20,	10,	58,	1,		54,	'1:22.589',	12,	230.064,	1),
        (51,	3,	24,	14,	11,	58,	0,		55,	'1:22.541',	11,	230.198,	1),
        (52,	3,	18,	19,	12,	58,	0,		48,	'1:23.592',	17,	227.304,	1),
        (53,	3,	47,	15,	13,	57,	0,	55,	'1:23.006',	14,	228.908,	11),
        (54,	3,	20,	16,	14,	57,	0,		44,	'1:23.071',	15,	228.729,	11),
        (55,	3,	22,	13,	15,	57,	0,		56,	'1:23.342',	16,	227.985,	11),
        (56,	3,	6,	18	,16	,57	,0		,49	,'1:23.882',18	,226.518	,11),
        (57,	3,	14,	10,	17,	57,	0,		57,'1:20.846',	2	,235.024	,11),
        (58,	3,	1,	2,	18,	38,	0,		37,'1:21.677',	6	,232.633	,95),
        (59,	3,	5,	17,	19,	22,	0,	17,'1:25.189',	19,	223.042,	3),
        (60,	3,	55,	9,	20,	1,	0,		None,	None,	0,	None,	20),
        (61,	4,	1,	1,	1,	63,	26,		55,	'1:18.446',	1,	225.281,	1),
        (62,	4,	11,	3,	2,	63,	18,	52,	'1:18.949',	3,	223.845,	1),
        (63,	4,	4,	5,	3,	63,	15,		61,	'1:20.903',	7,	218.439,	1),
        (64,	4,	63,	11,	4,	63,	12,		57,	'1:20.962',	8,	218.280,	1),
        (65,	4,	77	,7	,5	,63	,10,		43,	'1:20.758',	6,	218.831,	1),
        (66,	4,	16,	2,	6,	63,	8,		63,	'1:18.574',	2,	224.914,	1),
        (67,	4,	22	,12,	7,	63,	6,	61, '1:20.544',	5,	219.412,	1),
        (68,	4,	5	,13,	8,	63,	4, 	47, '1:21.211',	9,	217.610,	1),
        (69,	4,	20	,8	,9	,63	,2	, 61	,'1:21.238	',10,	217.538,	1),
        (70,	4,	18,	15,	10,	62,	1,		46,	'1:21.750',	16,	216.176,	11),
        (71,	4,	23,	18,	11,	62,	0,		61,	'1:21.757',	17,	216.157,	11),
        (72,	4,	10,	17,	12,	62,	0,		34,	'1:21.713',	15,	216.274,	11),
        (73,	4,	44,	14,	13,	62,	0,		50,	'1:21.419',	13,	217.054,	11),
        (74,	4,	31,	16,	14,	62,	0,	37,	'1:21.887',	18,	215.814,	11),
        (75,	4,	24,	0,	15,	62,	0,		61,	'1:21.286',	11,	217.410,	11),
        (76,	4,	6	,19,	16,	62,	0,	62,	'1:21.338',12,	217.271,	11),
        (77,	4,	47,	10,	17,	62,	0,	55,'1:18.999',	4,	223.704,	11),
        (78,	4,	3,	6,	18,	62,	0,		61,	'1:21.577', 14,	216.634,	11),
        (79,	4,	14,	9,	19,	6,	0,		5, '1:39.685',	19,	177.282	,130),
        (80,	4,	55,	4	,20,	0,	0,		None,	None,	0,	None,	4),
        (81,	5,	1,	3,	1,	57,	26,		54,	'1:31.361',	1,	213.255	,1),
        (82,	5,	16,	1,	2,	57,	18,	53,	'1:31.488',	2,	212.959,	1),
        (83,	5,	55,	2,	3,	57,	15,	56,	'1:31.790',	3,	212.258,	1),
        (84,	5,	11	,4	,4	,57	,12	,	54,	'1:31.819',	4,	212.191,	1),
        (85,	5,	63,	12,	5,	57,	10,		56,	'1:32.195',	5,	211.325,	1),
        (86,	5,	44,	6	,6,	57,	8,	55,'1:32.941',	7,	209.629,	1),
        (87,	5,	77,	5	,7	,57	,6	,56	,'1:33.184',10,	209.083,	1),
        (88,	5,	31,	20,	8,	57,	4,	56,'1:33.163',	9,	209.130,	1),
        (89,	5,	23,	18,	9	,57	,2	,57	,'1:33.447',15,	208.494,	1),
        (90,	5,	18,	0	,10	,57	,1	,52	,'1:33.312',12,	208.796,	1),
        (91,	5,	14,	11,	11	,57	,0	,53	,'1:33.331' ,13,	208.753,	1),
        (92,	5,	22,	9	,12,	57,	0,	55,'1:33.035',	8,	209.417,	1),
        (93,	5,	3	,14	,13	,57	,0	,56	,'1:33.265',11,	208.901,	1),
        (94,	5,	6	,19	,14	,57	,0	,53	,'1:34.169',18,	206.896,	1),
        (95,	5,	47,	15,	15,	57,	0,		57,	'1:32.528',	6,	210.565,	1),
        (96,	5,	20,	16,	16,	56,	0,52	,'1:33.511',17	,208.351	,33),
        (97,	5,	5,	0,	17,	54,	0,		50,'1:33.479',	16,	208.423,	4),
        (98,	5,	10,	7,	18,	45	,0		,38	,'1:34.487',19	,206.199	,22),
        (99,	5,	4,	8,	19,	39,	0,	37,'1:33.411',	14,	208.575,	4),
        (100,	5,	24,	17,	20,	6,	0,		4	,'1:35.731',20	,203.520	,47),
        (101,	6,	1,	2,	1,	66,	25,		46,	'1:25.456',	4,	196.943,	1),
        (102,	6,	11	,5	,2	,66	,19,	55,	'1:24.108',	1,	200.099,	1),
        (103,	6,	63	,4	,3	,66	,15,		53,	'1:24.636',	3,	198.851,	1),
        (104,	6,	55	,3	,4	,66	,12,		49,	'1:25.985',	7,	195.731,	1),
        (105,	6,	44	,6	,5	,66	,10,		51,	'1:24.253',	2,	199.755,	1),
        (106,	6,	77,	7,	6,	66,	8,		36,	'1:26.395',	8,	194.802,	1),
        (107,	6,	31,	12,	7,	66,	6,		54,	'1:25.935',	6,	195.845,	1),
        (108,	6,	4,	11,	8,	66,	4,		53,	'1:25.619',	5,	196.568,	1),
        (109,	6,	14,	20,	9,	65,	2,	56, '1:26.599',	9,	194.344,	11),
        (110,	6,	22,	13	,10	,65	,1		,59,	'1:26.828',	10,	193.831,	11),
        (111,	6,	5,	16	,11	,65	,0		,59,	'1:27.629',	18,	192.059,	11),
        (112,	6,	3,	9	,12	,65	,0		,54,	'1:27.285',	15,	192.816,	11),
        (113,	6,	10,	14	,13	,65	,0		,51,	'1:26.987',	12,	193.477,	11),
        (114,	6,	47,	10	,14	,65	,0		,32,	'1:27.447',	16,	192.459,	11),
        (115,	6,	18,	17	,15	,65	,0		,54,	'1:26.876',	11,	193.724,	11),
        (116,	6,	6,	19	,16	,64	,0		,52,	'1:27.246',	14,	192.902,	12),
        (117,	6,	20,	8,	17,	64,	0,		3,	'1:27.537',	17,	192.261,	12),
        (118,	6,	23,	18	,18	,64	,0		,56,	'1:28.281',	19,	190.641,	12),
        (119,	6,	24,	15	,19	,28	,0		,12,	'1:28.415',	20,	190.352,	75),
        (120,	6,	16,	1,	20,	27,	0,	25,	'1:27.030',	13,	193.381,	101),
        (121,	7,	11,	3,	1,	64,	25,		46,	'1:16.028',	4,	158.010,	1),
        (122,	7,	55,	2,	2,	64	,18,		47,'1:16.421',	7,	157.197,	1),
        (123,	7,	1	,4,	3	,64	,15,		47,'1:16.052',	5,	157.960,	1),
        (124,	7,	16,	1	,4,	64	,12,		46,'1:16.249',	6,	157.552,	1),
        (125,	7,	63,	6	,5,	64,	10,		42,'1:16.830',	8,	156.360,	1),
        (126,	7,	4	,5,	6,	64,	9,		55,'1:14.693',	1,	160.834,	1),
        (127,	7,	14,	7,	7,	64,	6,		50,'1:15.882',	3,	158.314,	1),
        (128,	7,	44,	8,	8,	64,	4,		51,'1:17.203',	9,	155.605,	1),
        (129,	7,	77,	12,	9	,64,	2	,57	,'1:17.600	',14,	154.809,	1),
        (130,	7,	5	,9,	10,	64,	1	,54	,'1:17.558',12,	154.893,	1),
        (131,	7,	10,	17,	11,	64,	0,	61	,'1:17.344',10,	155.321,	1),
        (132,	7,	31,	10	,12	,64,	0,	50	,'1:17.571',13,	154.867,	1),
        (133,	7,	3,14,13,	64	,0,	59	,'1:17.532',11,	154.945,	1),
        (134,	7,	18,	18,	14,	64,	0,		56,	'1:17.672',	15,	154.665,	1),
        (135,	7,	6	,19	,15	,63	,0	,43	,'1:18.579',18	,152.880	,11),
        (136,	7,	24	,20	,16	,63	,0	,52	,'1:18.200',17	,153.621	,11),
        (137,	7,	22,	11,	17,	63,	0,		61,'1:15.334',	2	,159.465	,11),
        (138,	7,	23	,16	,18	,48	,0		,40	,'1:18.023',16	,153.969	,26),
        (139,	7,	47,	15,	19,	24,	0,		24,'1:24.778',	19,	141.701,	3),
        (140,	7,	20	,13	,20	,19	,0		,18	,'1:33.754',20	,128.135	,34),
        (141,	8,	1,	3,	1,	51,	25,		50,	'1:46.050',	2,	203.779,	1),
        (142,	8,	11,	2,	2,	51,	19,		36,	'1:46.046',	1,	203.787,	1),
        (143,	8,	63,	5,	3,	51,	15,		42,	'1:47.177',	4,	201.636,	1),
        (144,	8,	44,	7,	4	,51,	12,	39,'1:47.044',	3	,201.887,	1),
        (145,	8,	10,	6,	5,	51,	10	,39	,'1:48.519',16	,199.143,	1),
        (146,	8,	5	,9	,6	,51	,8	,41	,'1:48.206',12	,199.719,	1),
        (147,	8,	14,	10,	7,	51,	6,	49,'	1:47.989',	8	,200.120,	1),
        (148,	8,	3	,12,	8,	51,	4	,44	,'1:48.276',13	,199.589,	1),
        (149,	8,	4	,11	,9,	51,	2,	37,'	1:47.997',	9	,200.105,	1),
        (150,	8,	31,	13,	10,	51,	1	,36	,'1:48.297',14	,199.551,	1),
        (151,	8,	77,	15,	11,	50,	0,		42,'1:48.179',	11,	199.768	,11),
        (152,	8,	23,	17,	12,	50,	0,		48,'1:47.966',	7,	200.163,	11),
        (153,	8,	22,	8	,13	,50	,0		,42	,'1:47.523	',5	,200.987,	11),
        (154,	8,	47,	20,	14,	50,	0,		40,'1:48.410',	15,	199.343,	11),
        (155,	8,	6	,18	,15	,50	,0		,37	,'1:49.583	',20,	197.209,	11),
        (156,	8,	18,	19,	16,	46,	0,	38,'1:48.038',	10,	200.029	,76),
        (157,	8,	20,	16,	17,	31,	0,		12,'	1:48.789',	18,	198.648	,131),
        (158,	8,	24,	14,	18,	23,	0,	12,'	1:48.723',	17,	198.769	,9),
        (159,	8,	16,	1	,19	,21	,0	,13	,'1:47.531	',6	,200.972,	131),
        (160,	8,	55,	4	,20	,8,	0,		3,	'1:48.978',	19,	198.304,	9),
        (161,	9,	1,	1,	1,	70,	25,	64,	'1:15.839',	2,	207.012,	1),
        (162,	9,	55,	3,	2,	70,	19,		63,	'1:15.749',	1,	207.258,	1),
        (163,	9,	44,	4,	3,	70,	15,		69,'1:16.167',	4	,206.120,	1),
        (164,	9,	63,	8,	4,	70,	12,	63,'1:16.418',	5	,205.443,	1),
        (165,	9,	16,	19,	5,	70,	10,	62,'1:15.901',	3	,206.843,	1),
        (166,	9,	31,	7,	6	,70,	8,	62,'1:17.110',	9	,203.600,	1),
        (167,	9,	77,	11,	7,	70,	6,	64,'1:17.010',	8	,203.864,	1),
        (168,	9,	24,	10,	8,	70,	4,	60,'1:16.927',	7	,204.084,	1),
        (169,	9,	14,	2,	9,	70,	2,	63,'1:16.578',	6	,205.014,	1),
        (170,	9,	18,	17,	10,	70,	1,	64	,'1:17.421',10	,202.782,	1),
        (171,	9,	3	,9,	11,	70,	0,	59	,'1:17.932',13	,201.452,	1),
        (172,	9,	5	,16,	12,	70,	0,	63	,'1:17.956',15	,201.390,	1),
        (173,	9,	23,	12,	13,	70,	0,	59	,'1:17.951',14	,201.403,	1),
        (174,	9,	10,	15,	14,	70	,0,	60	,'1:17.810',12	,201.768,	1),
        (175,	9,	4	,14	,15,	70,	0,	63	,'1:17.495',11	,202.588,	1),
        (176,	9,	6	,18	,16,	70,	0,	61	,'1:18.540',18	,199.893,	1),
        (177,	9,	20,	5,	17	,70	,0,		11,	'1:18.046',	16,	201.158	,1),
        (178,	9,	22,	20,	18,	47	,0	,15,	'1:18.309',	17,	200.482,	3),
        (179,	9,	47,	6	,19	,18	,0		,13,	'1:18.967',	20,	198.812,	5),
        (180,	9,	11,	13,	20,	7,	0,	7,	'1:18.844',	19,	199.122,	6),
        (181,	10,	55,	1,	1,	52,	25,		44,	'1:30.813',	2,	233.530,	1),
        (182,	10,	11,	4,	2,	52,	18,		47,'1:30.937',	3	,233.212,	1),
        (183,	10,	44,	5,	3,	52,	16,		52,'1:30.510',	1	,234.312,	1),
        (184,	10,	16,	3,	4,	52,	12,		52,'1:31.282',	4	,232.330,	1),
        (185,	10,	14,	7,	5,	52,	10,		47,'1:31.609',	5	,231.501,	1),
        (186,	10,	4	,6,	6,	52,	8,		47,'1:31.645',	6	,231.410,	1),
        (187,	10,	1	,2,	7,	52,	6,		44,'1:32.354',	8	,229.633,	1),
        (188,	10,	47,	19,	8,	52,	4,		48,'1:32.109',	7	,230.244,	1),
        (189,	10,	5	,18,	9,	52,	2,	52	,'1:32.471	',10	,229.343,	1),
        (190,	10,	20,	17	,10,	52,	1,	52	,'1:32.661	',12	,228.872,	1),
        (191,	10,	18,	20,	11,	52,	0,		52,'1:32.379',	9	,229.571,	1),
        (192,	10,	6	,10,	12,	52,	0,	48	,'1:33.286	',13	,227.339,	1),
        (193,	10,	3	,14,	13,	52,	0,	34	,'1:32.644	',11	,228.914,	1),
        (194,	10,	22,	13,	14,	52,	0,	51	,'1:33.832	',15	,226.016,	1),
        (195,	10,	31,	15,	15,	37,	0,      37,	'1:33.537',	14,	226.729,	48),
        (196,	10,	10,	11,	16,	26,	0,		18,	'1:34.614',	16,	224.148,	130),
        (197,	10,	77,	12,	17,	20,	0,		19,	'1:35.103',	17,	222.996,	6),
        (198,	10,	63,	8	,18,	0,	0,	None,	None,	0,	None,	4),
        (199,	10,	24,	9	,19,	0,	0,	None,	None,	0,	None,	4),
        (200,	10,	23	,16	,20,	0,	0,	None,	None,	0,	None,	4),
        (201,	11,	16,	2,	1,	71,	25,		62,	'1:07.583',	2,	230.010,	1),
        (202,	11,	1,	1,	2,	71,	19,	62,	'1:07.275',	1,	231.063,	1),
        (203,	11,	44,	8,	3,	71,	15	,	63,	'1:09.000',	5,	225.286,	1),
        (204,	11,	63,	4,	4,	71,	12	,	61,	'1:09.075',	6,	225.042,	1),
        (205,	11,	31,	6,	5,	71,	10,		62,	'1:09.559',	10,	223.476,	1),
        (206,	11,	47,	9	,6,	70,	8		,50	,'1:09.625',11	,223.264,	11),
        (207,	11,	4	,10	,7,	70,	6,	62,'1:09.304',	8	,224.298,	11),
        (208,	11,	20,	7	,8,	70,	4		,61	,'1:09.938',14	,222.265,	11),
        (209,	11,	3	,11	,9,	70,	2		,61	,'1:09.924' ,13	,222.309,	11),
        (210,	11,	14,	19,	10,	70,	1,	62,'1:08.558',	3	,226.739,	11),
        (211,	11,	77,	0	,11,	70,	0,	61,'1:09.266',	7	,224.421,	11),
        (212,	11,	23,	15,	12	,70,	0		,60	,'1:09.669',12	,223.123,	11),
        (213,	11,	18,	12,	13	,70,	0		,64	,'1:10.048',17	,221.916,	11),
        (214,	11,	24,	13,	14,	70,	0,		50,'1:09.380',	9	,224.053,	11),
        (215,	11,	10,	14,	15,	70,	0		,61	,'1:10.104',18	,221.739,	11),
        (216,	11,	22,	16,	16,	70,	0		,62	,'1:10.023',16	,221.995,	11),
        (217,	11,	5	,18	,17,	70,	0		,62	,'1:10.001',15	,222.065,	11),
        (218,	11,	55,	3,	18,	56,	0,		55,'1:08.649',	4	,226.438,	131),
        (219,	11,	6	,17,	19,	48	,0		,14	,'1:10.890',19	,219.280,	140),
        (220,	11,	11,	5	,20,	24,	0,		5	,'1:11.843',20	,216.371,	130),
        (221,	12,	1	,2,	1,	53,	25,		30,	'1:37.491',	2,	215.724,	1),
        (222,	12,	44,	4,	2,	53,	18,	30,'1:37.668',	4	,215.333,	1),
        (223,	12,	63,	6,	3,	53,	15,	51,'1:37.548',	3	,215.598,	1),
        (224,	12,	11,	3,	4,	53,	12,	45,'1:37.780',	5	,215.086,	1),
        (225,	12,	55,	19,	5,	53,	11,	51,'1:35.781',	1	,219.575,	1),
        (226,	12,	14,	7	,6,	53,	8,	53,'1:38.160',	8	,214.254,	1),
        (227,	12,	4	,5,	7,	53,	6	,43	,'1:39.037	',11	,212.356,	1),
        (228,	12,	31,	10,	8,	53,	4,	53,'1:38.684',	9	,213.116,	1),
        (229,	12,	3,	9,	9,	53	,2	,27	,'1:39.133',14,	212.151,	1),
        (230,	12,	18,	15	,10	,53	,1	,52	,'1:39.185',15,	212.040,	1),
        (231,	12,	5	,12	,11	,53	,0	,43	,'1:39.044',12,	212.341,	1),
        (232,	12,	10	,14	,12	,53	,0	,53	,'1:38.786',10,	212.896,	1),
        (233,	12,	23,	13	,13	,53	,0	,53	,'1:39.199',16,	212.010,	1),
        (234,	12,	77,	11,	14,	53,	0,52,'1:37.963',	6,	214.685,	1),
        (235,	12,	47,	17	,15	,53	,0	,48	,'1:39.068',13,	212.290,	1),
        (236,	12,	24,	16,	16,	47,	0,	25,	'1:39.368',	18,	211.649,	16),
        (237,	12,	6	,18	,17	,40	,0		,40	,'1:39.650',	19	,211.050,	130),
        (238,	12,	20,	20,	18,	37,	0,		11	,'1:39.265',	17,	211.869,	130),
        (239,	12,	16,	1	,19	,17	,0		,4	,'1:38.088',	7,	214.411,	3),
        (240,	12,	22,	8	,20	,17	,0		,4	,'1:40.216',	20,	209.858,	140),
        (241,	13,	1,	10,	1,	70,	25,		45,	'1:22.126',	6,	192.041,	1),
        (242,	13,	44,	7,	2,	70,	19,	57,	'1:21.386',	1,	193.787,	1),
        (243,	13,	63,	1	,3	,70	,15	,	57,	'1:22.766',	9,	190.556,	1),
        (244,	13,	55,	2	,4	,70	,12	,	51,	'1:22.000',	4,	192.336,	1),
        (245,	13,	11,	11	,5	,70	,10	,	44,	'1:21.940',	3,	192.477,	1),
        (246,	13,	16,	3,	6,	70,	8,		57,	'1:21.622',	2,	193.227,	1),
        (247,	13,	4,	4,	7,	70,	6,		47,	'1:23.043',	11,	189.920,1),
        (248,	13,	14,	6	,8,	69,	4,		47,	'1:23.979',	18,	187.804,	11),
        (249,	13,	31,	5,	9,	69,	2,		60,	'1:24.149',	20,	187.424,	11),
        (250,	13,	5	,18,	10,	69,	1,		51,	'1:22.824',	10,	190.423,	11),
        (251,	13,	18,	14,	11,	69,	0,		51,	'1:22.437',	7,	191.317,	11),
        (252,	13,	10,	0,	12,	69,	0,		58,	'1:23.199',	14,	189.564,	11),
        (253,	13,	24,	12,	13,	69,	0,		58,	'1:22.029',	5,	192.268,	11),
        (254,	13,	47,	15,	14,	69,	0,		50,	'1:23.151',	13,	189.674,	11),
        (255,	13,	3,   9,	15,	69,	0,		63,	'1:23.654',	17,	188.533,	11),
        (256,	13,	20,	13,	16,	69,	0,  	37,	'1:23.511',	15,	188.856,	11),
        (257,	13,	23,	17,	17,	69,	0,		43,	'1:23.047',	12,	189.911,	11),
        (258,	13,	6	,19	,18,	69,	0,	60,	'1:22.478',	8,	191.221,	11),
        (259,	13,	22,	16,	19,	68,	0,  	58,	'1:23.538',	16,	188.795,	12),
        (260,	13,	77,	8,	20,	65,	0,		60,	'1:24.002',	19,	187.752,    131)]
try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_results,results_List)
    # commit the changes to the database
    con.commit()

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while exccuting the query at PostgreSQL",error)
    
finally:
    
    count = cursor.rowcount
    print (count, "Record inserted successfully into results table")

#create stats eventd table
 
try:
    #table_name variable
    statseventsTable="statsevents"
    create_statseventsTable_query = '''CREATE TABLE '''+ statseventsTable+''' 
              (statsevent_id INT  PRIMARY KEY     NOT NULL,
               statsevent_description TEXT,
               statsevent_measure  TEXT 
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_statseventsTable_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ statseventsTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)   
    
    
# insert data in stats events table
sql_insert_orders = '''INSERT INTO "statsevents" (statsevent_id,statsevent_description,statsevent_measure) VALUES(%s,%s,%s)'''

orders_List=[
        (1,	'Best time qualifying',	'time (h:mm:ss.ms)'),
        (2,	'Best time in race	time', '(h:mm:ss.ms)'),
        (3,	'Best time in Pit Stop',	'time (ss.ms)'),
        (4,	'Best time in Pit Stop (Pit Lane)',	'time (ss.ms)'),
        (5,	'First retired from the race',	'laps completed'),
        (6,	'Last driver (a pieni giri)',	'delay time (mm:ss.ms)'),
        (7,	'Highest penalty imposed (time)',	'time (seconds)'),
        (8,	'Highest penalty imposed (positions)',	'number'),
        (9,	'Greatest number of overtakes',	'number'),
        (10,	'Most positions gained',	'number')]
try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_orders,orders_List)
    # commit the changes to the database
    con.commit()

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while exccuting the query at PostgreSQL",error)
    
finally:
    
    count = cursor.rowcount
    print (count, "Record inserted successfully into statsevents table")

    #Create "stats" Table


try:
    #table_name variable
    statsTable="stats"
    create_statsTable_query = '''CREATE TABLE '''+ statsTable+''' 
              (stats_id INT  PRIMARY KEY     NOT NULL,
               granprix_id  INT  REFERENCES granprix(granprix_id) NOT NULL,
               stats_championshipyear INT,
               driver_id  INT  REFERENCES drivers(driver_id) NOT NULL,
               statsevent_id  INT  REFERENCES statsevents(statsevent_id) NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_statsTable_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ statsTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)


#insert data into stats table

sql_insert_stats = '''INSERT INTO "stats" (stats_id,stats_championshipyear,granprix_id,statsevent_id,driver_id) VALUES(%s,%s,%s,%s,%s)'''

#list of customers
stats_List=[
        (1,	2022,	1,	1,	16),
        (2,	2022,	1,	2,	16),
        (4,	2022,	1,	3,	5),
        (14,	2022,	2,	3,	16),
        (5,	2022,	1,	5,	10),
        (6,	2022,	1,	6,	27),
        (7,	2022,	1,	7,	22),
        (8,	2022,	1,	8,	24),
        (9,	2022,	1,	9,	22),
        (10,	2022,	1,	10,	22),
        (11,	2022,	2,	1,	11),
        (12,	2022,	2,	2,	16),
        (19,	2022,	3,	3,	11),
        (3,	2022,	1,	4,	44),
        (15,	2022,	2,	5,	22),
        (16,	2022,	2,	6	,27),
        (17,	2022,	2,	7	,20),
        (13,	2022,	2,	4	,3),
        (20,	2022,	2,	10,	44),
        (21,	2022,	3,	1	,16),
        (22,	2022,	3,	2	,16),
        (23,	2022,	3,	4	,18),
        (25,	2022,	3,	5	,55),
        (26,	2022,	3,	6	,19),
        (27,	2022,	3,	7	,24),
        (29,	2022,	3,	9	,23),
        (30,	2022,	3,	10,	23),
        (31,	2022,	4,	2	,1),
        (32,	2022,	5,	2	,1),
        (33,	2022,	6,	2	,11),
        (34,	2022,	7,	2	,4),
        (35,	2022,	8,	2	,11),
        (36,	2022,	9,	2	,55),
        (37,	2022,	10,	2,	44),
        (38,	2022,	11,	2,	1),
        (39,	2022,	12,	2,	55),
        (40,	2022,	13,	2,	44),
        (41,	2022,	4	,10,	63),
        (42,	2022,	5	,10,	23),
        (43,	2022,	6	,10,	31),
        (44,	2022,	7	,10,	10),
        (45,	2022,	8	,10,	47),
        (46,	2022,	9	,10,	16),
        (47,	2022,	10,	10,	47),
        (48,	2022,	11,	10,	19),
        (49,	2022,	12,	10,	19),
        (50,	2022,	13,	10,	10),
        (51,	2022,	3	,9	,23),
        (52,	2022,	4	,9	,63),
        (53,	2022,	5	,9	,23),
        (54,	2022,	6	,9	,31),
        (55,	2022,	7	,9	,10),
        (56,	2022,	8	,9	,47),
        (57,	2022,	9	,9	,16),
        (58,	2022,	10,	9,	47),
        (59,	2022,	11,	9,	19),
        (60,	2022,	12,	9,	19),
        (61,	2022,	13,	9,	10),
        (62,	2022,	4	,6	,20),
        (63,	2022,	5	,6	,47),
        (64,	2022,	6	,6	,4),
        (65,	2022,	7	,6	,18),
        (66,	2022,	8	,6	,31),
        (67,	2022,	9	,6	,20),
        (68,	2022,	10,	6,	22),
        (69,	2022,	11,	6,	31),
        (70,	2022,	12,	6,	47),
        (71,	2022,	13,	6,	4),
        (72,	2022,	2	,5	,22),
        (73,	2022,	4	,5	,55),
        (74,	2022,	5	,5	,24),
        (75,	2022,	6	,5	,16),
        (76,	2022,	7	,5	,20),
        (77,	2022,	8	,5	,55),
        (78,	2022,	9	,5	,11),
        (79,	2022,	10,	5,	23),
        (80,	2022,	11,	5,	11),
        (81,	2022,	12,	5,	22),
        (82,	2022,	13,	5,	77),
        (83,	2022,	10,	5,	63),
        (84,	2022,	10,	5,	24),
        (85,	2022,	11,	5,	6),
        (86,	2022,	11,	5,	55),
        (87,	2022,	12,	5,	16),
        (88,	2022,	4	,1	,1),
        (89,	2022,	5	,1	,16),
        (90,	2022,	6	,1	,16),
        (91,	2022,	7	,1	,16),
        (92,	2022,	8	,1	,16),
        (93,	2022,	9	,1	,1),
        (94,	2022,	10,	1,	55),
        (95,	2022,	11,	1,	1),
        (96,	2022,	12,	1,	16),
        (97,	2022,	13,	1,	63),
        (98,	2022,	4	,4	,3),
        (99,	2022,	5	,4	,22),
        (100,	2022,	6	,4	,44),
        (101,	2022,	7	,4	,18),
        (102,	2022,	8	,4	,6),
        (103,	2022,	9	,4	,5),
        (104,	2022,	10,	4,	1),
        (105,	2022,	11,	4,	11),
        (106,	2022,	12,	4,	20),
        (107,	2022,	13,	4,	23),
        (108,	2022,	4	,3	,3),
        (109,	2022,	5	,3	,22),
        (110,	2022,	6	,3	,44),
        (111,	2022,	7	,3	,18),
        (112,	2022,	8	,3	,6),
        (113,	2022,	9	,3	,5),
        (114,	2022,	10,	3,	1),
        (115,	2022,	11,	3,	11),
        (116,	2022,	12,	3,	20),
        (117,	2022,	13,	3,	23)]
try:
    # execute the INSERT statement
    cursor.executemany(sql_insert_stats,stats_List)
    # commit the changes to the database
    con.commit()

except (Exception, psycopg2.Error) as error :
    con.rollback()
    print ("Error while exccuting the query at PostgreSQL",error)
    
finally:
    
    count = cursor.rowcount
    print (count, "Record inserted successfully into stats table")