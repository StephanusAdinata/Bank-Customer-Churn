	create table table_m3(
	id serial primary key,
	"RowNumber" int,
	"CustomerId" int,
	"Surname" varchar(50),
	"CreditScore" int,
	"Geography" varchar(50),
	"Gender" varchar(50),
	"Age" int,
	"Tenure" int,
	"Balance" float,
	"NumOfProducts" int,
	"HasCrCard" int,
	"IsActiveMember" int,
	"EstimatedSalary" float,
	"Exited" int
);

copy table_m3("RowNumber", "CustomerId", "Surname", "CreditScore", "Geography", "Gender", "Age",
				 "Tenure", "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary", "Exited")
FROM 'C:\tmp\P2M3_Stephanus_data_raw.csv'
DELIMITER ','
CSV HEADER;