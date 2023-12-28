CREATE KEYSPACE ineuron
	WITH REPLICATION = {
		'class': 'org.apache.cassandra.locator.SimpleStrategy',
		'replication_factor': '3'
	}
	AND DURABLE_WRITES = true;


-- CREATE TABLE ineuron.EMPLOYEE(
--     EMP_ID INT,
--     EMP_NAME text,
--     CITY text,
--     STATE text,
--     primary key (EMP_ID)
-- );



-- select * from ineuron.EMPLOYEE

-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (1,'Stephen','Bengalore','Karnataka');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (2,'Kholoud','Cairo city','Cairo');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (3,'Es Rosario','Global city','Manila');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (4,'Stephen','Mumbai','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (5,'Berry','Chennai','Tamilnadu');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (6,'Barton','Pune','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (7,'Natasha','Hyderabad','Andra Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (8,'Sundar','Noida','Uttar Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (9,'Shashank','Delhi','Delhi');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (10,'Sudhanshu','New Mumbai','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (11,'Krish','Nagpur','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (12,'Aman','Hyderabad','Andra Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (13,'Rahul','Noida','Uttar Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (14,'Sunny','Delhi','Delhi');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (15,'Vishal','New Mumbai','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (16,'Vikash','Nagpur','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (17,'Aravind','Chennai','Tamilnadu');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (18,'Adam','Pune','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (19,'Scarlett','Hyderabad','Andra Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (20,'Robert','Noida','Uttar Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (21,'Shivam','Delhi','Delhi');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (22,'Deepak','New Mumbai','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (23,'Amit','Nagpur','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (24,'Susmita','Hyderabad','Andra Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (25,'Iris','Mumbai','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (26,'Pia','Chennai','Tamilnadu');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (27,'Ankit','Pune','Maharashtra');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (28,'Akash','Hyderabad','Andra Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (29,'Harry','Noida','Uttar Pradesh');
-- INSERT INTO ineuron.EMPLOYEE (EMP_ID,EMP_NAME,CITY,STATE) VALUES (30,'Emma','Delhi','Delhi');
