CREATE TABLE IF NOT EXISTS Customers (
   InvoiceNo Varchar(255) NOT NULL,
   StockCode Varchar(255) NOT NULL,
   Description Varchar(255) ,
   Quantity int , 
   InvoiceDate Varchar(255) NOT NULL,
   UnitPrice NUMERIC ,
   CustomerID int NOT NULL,
   Country Varchar(255)
);