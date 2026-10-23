
CREATE DATABASE SalesAnalyticsDataWarehouse;
GO

USE SalesAnalyticsDataWarehouse;
GO


CREATE OR ALTER VIEW v_FactSales AS
SELECT 
    CAST(OrderID AS INT) AS OrderID,
    CAST(DateKey AS INT) AS DateKey,
    CAST(CustomerSK AS BIGINT) AS CustomerSK,
    CAST(ProductSK AS BIGINT) AS ProductSK,
    CAST(RegionSK AS BIGINT) AS RegionSK,
    CAST(Quantity AS INT) AS Quantity,
    CAST(UnitPrice AS DOUBLE PRECISION) AS UnitPrice,
    CAST(TotalPrice AS DOUBLE PRECISION) AS TotalPrice,
    CAST(IngestedAt AS DATE) AS IngestedAt
FROM OPENROWSET(
    BULK {STORAGE_ACCOUNT_URL}/gold/FactSales/,
    FORMAT = 'DELTA'
) AS [fact];
GO


CREATE OR ALTER VIEW v_DimCustomer AS
SELECT 
    CAST(CustomerSK AS BIGINT) AS CustomerSK,
    CAST(CustomerID AS INT) AS CustomerID,
    CAST(CustomerName AS VARCHAR(255)) AS CustomerName,
    CAST(CustomerEmail AS VARCHAR(255)) AS CustomerEmail,
    CAST(StartDate AS DATE) AS StartDate,
    CAST(EndDate AS DATE) AS EndDate,
    CAST(IsCurrent AS BIT) AS IsCurrent
FROM OPENROWSET(
    BULK {STORAGE_ACCOUNT_URL}/gold/DimCustomer/,
    FORMAT = 'DELTA'
) AS [cust];
GO


CREATE OR ALTER VIEW v_DimProduct AS
SELECT 
    CAST(ProductSK AS BIGINT) AS ProductSK,
    CAST(ProductID AS INT) AS ProductID,
    CAST(ProductName AS VARCHAR(255)) AS ProductName,
    CAST(UnitPrice AS DOUBLE PRECISION) AS UnitPrice,
    CAST(UpdatedAt AS DATE) AS UpdatedAt
FROM OPENROWSET(
    BULK {
    STORAGE_ACCOUNT_URL}/gold/DimProduct/,
    FORMAT = 'DELTA'
) AS [prod];
GO

CREATE OR ALTER VIEW v_DimSalesRegion AS
SELECT 
    CAST(RegionSK AS BIGINT) AS RegionSK,
    CAST(Country AS VARCHAR(255)) AS Country,
    CAST(SalesRegion AS VARCHAR(255)) AS SalesRegion,
    CAST(InsertedAt AS DATE) AS InsertedAt
FROM OPENROWSET(
    BULK {STORAGE_ACCOUNT_URL}/gold/DimSalesRegion/,
    FORMAT = 'DELTA'
) AS [region];
GO


CREATE OR ALTER VIEW v_DimDate AS
SELECT 
    CAST(DateKey AS INT) AS DateKey,
    CAST(OrderDate AS DATE) AS OrderDate,
    CAST(OrderDateString AS VARCHAR(50)) AS OrderDateString,
    CAST(Day AS INT) AS Day,
    CAST(Month AS INT) AS Month,
    CAST(MonthName AS VARCHAR(50)) AS MonthName,
    CAST(Year AS INT) AS Year,
    CAST(DayOfWeek AS INT) AS DayOfWeek,
    CAST(DayName AS VARCHAR(50)) AS DayName,
    CAST(Quarter AS INT) AS Quarter
FROM OPENROWSET(
    BULK {STORAGE_ACCOUNT_URL}/gold/DimDate/,
    FORMAT = 'DELTA'
) AS [date];
GO