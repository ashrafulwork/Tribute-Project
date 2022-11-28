import psycopg2
from psycopg2 import Error

print("------This Project is made by group Alliance------------")

try:
    conn = psycopg2.connect(dbname = 'Crime Report', user = 'postgres',password = '1234',port ='5432')
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS report')



    #We have created our main  table (report) here
    table = ''' create table report(name_criminal varchar(1000),nickname_criminal varchar(1000) ,age_criminal int,id_criminal int, height_criminal float, crime_criminal varchar(1000),punishmentyear_criminal int,crimedate_criminal date,crimelocation_criminal varchar(1000))'''
    cursor.execute(table)



    #We have inserted table data here
    insert = ''' insert into report (name_criminal,nickname_criminal,age_criminal,id_criminal,height_criminal,crime_criminal,punishmentyear_criminal,crimedate_criminal,crimelocation_criminal)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

    # Criminal Data are given here. We have taken random info from different website and used some imaginary names
    values = [('Aminur Rasul Sagor','Tokai Sagor', 26,1, 5.3, 'Murder of a Public Officer', 7,'2000/10/09','Dist- Chittagong AP-House-29, Road-39, Gulshan, DMP,Dhaka. (AP:Vill- 412 Askuna, thana-dakhin Khan, Dhaka'),
              ('Jafor Ahmed','Manik',32,2,5.6,'Murder of a professor',8,'2010/07/09','Dist-Dhaka,AP:Vill-271, Uttar Shahjahanpur,,Thana-shahjahanpur,DMP,Dhaka'),
              ('Shamim Ahmed','Aga shamim',29,3,5.2,'Murder of a child',5,'2016/02/01','Vill- 41/Abdul Hadi Lane Thana- Kotwali,DMP,dhaka.'),
              ('Liakat Hossen','choto liakat' ,42,4, 5.7, 'Murder of a 30 year old woman', 5,'2020/01/01','Vill- Crokirchar, Thana-Shibchar,Dist-Madaripur.AP- 14/C, Mona towar Eskaton, Dhaka'),
              ('Imamul Hossain', 'helal or Pichi Helal', 47,5, 5.8, 'Vehicle Theft.', 2,'2010/04/01','Dist- Chandpur'),
              ('Trimoti Subrato Bain ','Suvra',37,6,5.2,'Murder of a 25 year old man',4,'2010/04/01','Dist- Gazipur.AP- 1/T/7 Mirbagh and 186 Nayatola,Mogbazar, Ramna, DMP Dhaka.'),
              ('Masud', 'Mulla Masud',27,7,5.6,'Murder of a 36 year old man',4,'2016/03/09','Vill-Mohadebpur, Thana+Dist-Jalkathi.AP-14/Mirbagh, Thana-Ramna,DMP,Dhaka .'),
              ('Arman','Sumon',24,8,5.1,'Murder of child',6,'2019/02/27','AP- C/O-Imtiaz Khan babul, D-3 Husing Quarter,Kazi Nazrul Islam road, PS-MohammadpurDMP, Dhaka .'),
              ('Abbas','Killer Abbas',25,9,5.3,'Robbery',3,'2009/12/27','Vill- 188 IbrahimpurThana-Kafrul, DMP, Dhaka.'),
              ('Abdul Jobbar Munna','Jobbar Mia',35,10,5.4,'Sexual Assault',2,'2020/11/20','Dist-Netrokuna.AP-18 Razi-Sultana Road, Thana-Mohammadpur, DMP, Dhaka.'),
              ('Prokash Kumar Biswas','Kumar jollad',38,11,5.5,'Murder of a Police Officer',4,'2020/06/30','Dist- Jinaidah.AP-417/3 (471/3) South Paikpara,PS-Mirpur, DMP, Dhaka.'),
              ('Imran Hossain','Patowari',44,12,6.2,'Murder of a Public Worker',7,'2020/01/11','Vill- GP Cha-39,Mohakhali Warless gate,DMP, Dhaka .'),
              ('Khorsed Alam','Freedom Rasu',30,13,6.0,'Burglary',1,'2020/01/11','Vill- 364/B, Siddesori Lane Thana- Ramna, Dhaka.(AP-21 Siddesori Lane Building No-03, Flat-104 eastern Plaza, Ramna, DMP, Dhaka).'),
              ('Khandakar Tanveer Islam ','Joy',48,14,5.7,'Burglary',1,'2022/01/31','Vil- Baniara, Thana- Mirzapur,Dist-Tangail.AP-438/16 (43/R/16) Indira Road,PS-Tajgaon, DMP, Dhaka.'),
              ('Sohel Rana Chowdhory','Freedom Sohel',46,15,5.0,'Burglary',2,'2021/09/22','Dist- Feni.AP- 1063, Malibagh bazar road,PS-Khilgaon, DMP, Dhaka.')






              ]

    for v in values:
        cursor.execute(insert,v)


    conn.commit()


    # We have started our table partition from here
    drop_one = 'drop table if exists partition_one'
    cursor.execute(drop_one)
    drop_two = 'drop table if exists partition_two'
    cursor.execute(drop_two)
    drop_three = 'drop table if exists partition_three'
    cursor.execute(drop_three)

    conn.commit()

    partition_one = '''create table partition_one as (select * from report where age_criminal> 20 and age_criminal< 30)'''
    cursor.execute(partition_one)
    print('partion_one executed')

    partition_two = '''create table partition_two as (select * from report where age_criminal >=30  and age_criminal< 40)'''
    cursor.execute(partition_two)
    print('partion_two executed')

    partition_three = '''create table partition_three as (select * from report where age_criminal >=40 and age_criminal< 50)'''
    cursor.execute(partition_three)
    print('partion_three executed')



    conn.commit()


    #We have started performing our queries


    #insertion

    # insert = ''' insert into partition_one(name_criminal, nickname_criminal,age_criminal,id_criminal,height_criminal,crime_criminal,punishmentyear_criminal,crimedate_criminal,crimelocation_criminal)
    #     values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    #
    # data = ('Kala Jahangir', 'Kala Manik', 40,16, 5.7, 'Vehicle Theft.', 2,'2011/04/01','Dist-dhaka.')
    # cursor.execute(insert, data)
    # print(cursor.fetchall())
    #
    # conn.commit()


    #selection
    cursor.execute('select * from partition_two where height_criminal< 5.7')
    print(cursor.fetchall())
    print('Executed selection operation')

    #projection
    cursor.execute('select name_criminal, nickname_criminal from partition_two')
    print(cursor.fetchall())
    print('Executed projection operation')

    # deletion
    cursor.execute("delete from partition_three where age_criminal= 35 ")
    cursor.execute('select * from partition_three')
    print(cursor.fetchall())
    print('Executed deletion operation')

    # update
    cursor.execute("update partition_three set height_criminal = 5.5 where age_criminal= 26")
    cursor.execute('select * from partition_three')
    print(cursor.fetchall())
    print('Executed update operation')

    # ordering
    cursor.execute('select name_criminal,age_criminal,id_criminal,crime_criminal from partition_three order by age_criminal desc')
    print(cursor.fetchall())
    print('Executed ordering operation')

    # grouping
    cursor.execute('select crime_criminal,count(punishmentyear_criminal) from partition_three group by crime_criminal')

    print(cursor.fetchall())
    print('Executed grouping operation')

    print('---------All the operations are done. Now we can exit from the python IDE and postgreSQL connection will be closed now-------------')













except (Exception, Error) as error:
    print('There are some problem : ',error)


finally:
    if (conn):
         cursor.close()
         conn.close()
         print('The postgresql connection is closed now')