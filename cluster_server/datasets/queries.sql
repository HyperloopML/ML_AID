-- 
-- clean data view
-- 
CREATE VIEW [dbo].[vw_clean]
AS
SELECT DISTINCT 
                         dbo.[tran].TransactionElemID AS TrElem, dbo.[tran].TransactionID AS Trans, dbo.[tran].ProductID AS Prod, dbo.[tran].CustomerID AS Cust, dbo.[tran].Date AS Dt, dbo.[tran].OrderTotal AS ProdTotal, 
                         dbo.[tran].LocationID AS Loc, dbo.cust.Age, dbo.cust.Sex, dbo.cust.State AS St, 
						 SUBSTRING(dbo.[tran].Date, 1, 4) AS Y, 
						 SUBSTRING(dbo.[tran].Date, 5, 2) AS M, 
						 SUBSTRING(dbo.[tran].Date, 7, 2) AS D, 
                         DATEDIFF(day, '20160101', dbo.[tran].Date) AS DateScore
FROM            dbo.[tran] INNER JOIN
                         dbo.cust ON dbo.[tran].CustomerID = dbo.cust.CustomerID

GO
-- END clean data view

-- 
-- RFM data view
--
CREATE VIEW [dbo].[vw_rfm]
AS
SELECT        Cust, Age, Sex, St, 
			  MAX(M) AS R_LastM, 
			  MAX(DateScore) AS R_Score, 
			  COUNT(Trans) AS F_NrTr, 
			  SUM(ProdTotal) AS M_TotSum, 
			  MAX(ProdTotal) AS MaxProdVal
FROM            dbo.vw_clean
GROUP BY Cust, Age, Sex, St
GO
-- END RFM data view

--
-- month_cust - monthly amount per customer
-- 

SELECT        Cust, Age, Sex, St, Y, M, sum(ProdTotal) MonthSum
FROM            vw_clean
group by Cust, Age, Sex, St, Y, M
order by Cust

--
-- month_cust_loc - monthly amount per customer & location
-- 

SELECT        Cust, Age, Sex, St, Y, M, Loc, sum(ProdTotal) MonthSum
FROM            vw_clean
group by Cust, Age, Sex, St, Y, M, Loc
order by Cust


--
-- month_cust_prod - monthly amount per customer & product
-- 

SELECT        Cust, Age, Sex, St, Y, M, Prod, sum(ProdTotal) ProdSumMonth
FROM            vw_clean
group by Cust, Age, Sex, St, Y, M, Prod
order by Cust


--
-- month_cust_prod_loc - monthly amount per customer & product, location
-- 

SELECT        Cust, Age, Sex, St, Y, M, Prod, Loc, sum(ProdTotal) ProdSumMonth
FROM            vw_clean
group by Cust, Age, Sex, St, Y, M, Prod, Loc
order by Cust


--- 
--- RFM_data - recency, frequency and monerary query 
---
SELECT        * from vw_RFM
order by M_TotSum

--
-- clean RFM data
--
SELECT * from vw_rfm where TotalValue<>0 order by TotalValue DESC

select datediff(day,'20160101','20160930')

SELECT cast(convert(char(8), CAST('20160930' as Date), 112) as int)

SELECT CAST( as int) - CAST(CAST('20160101' as Date) as int) As test