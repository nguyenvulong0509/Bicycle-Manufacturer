--lưu ý chung: chỉnh lại tên CTE thành raw_data hoặc gì đó, mình đặt CTE nó kì lắm, giống như mình đặt tên table là table vậy :D


/* Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
Output: Period (Month n Year), Product Name, Order quantity, Total sales (from total line), Number of order
Group by and Order by each subcategory, in last 12 months (find the lastest date then subtract 12) */
WITH raw_data AS(
SELECT FORMAT_DATE('%b %Y', c1.ModifiedDate) as period,
      c3.Name as Subcat,
      SUM(c1.OrderQty) as Q_of_items,
      ROUND(SUM(c1.LineTotal),2) as Total_sales,
      COUNT(c1.ProductID) as Q_of_orders
FROM `adventureworks2019.Sales.SalesOrderDetail` as c1
  LEFT JOIN `adventureworks2019.Production.Product` as c2
    USING(ProductID)
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as c3
    ON CAST(c2.ProductSubcategoryID as INT) = c3.ProductSubcategoryID
WHERE date(c1.ModifiedDate) >= DATE_SUB('2014-06-30', INTERVAL 12 MONTH)
--where date(a.ModifiedDate) >= (select date_sub(max(date(a.ModifiedDate)), INTERVAL 12 month) FROM `adventureworks2019.Sales.SalesOrderDetail`)
GROUP BY FORMAT_DATE('%b %Y', c1.ModifiedDate),
    c3.Name)

SELECT *
FROM raw_data
ORDER BY period DESC,
        Subcat;
--correct


/* Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal
output: period, name, qty-item, total_sales, order_cnt
Get nsubcat name, previous qty_item
YoY = (qty_item - previous qty_item)/ previous qty_item
Join the tables to get data 
limit top 3 and round the result*/
WITH raw_data AS(
SELECT c3.Name as Subcat,
      FORMAT_DATE('%Y',c1.ModifiedDate) as Year,
      SUM(c1.OrderQty) as qty_item,
FROM `adventureworks2019.Sales.SalesOrderDetail` as c1
  LEFT JOIN `adventureworks2019.Production.Product` as c2
    USING(ProductID)
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as c3
    ON CAST(c2.ProductSubcategoryID as INT) = c3.ProductSubcategoryID
GROUP BY c3.Name,
        FORMAT_DATE('%Y',c1.ModifiedDate))
,
prev_year AS(
SELECT *,
      LAG(qty_item) OVER(PARTITION BY Subcat ORDER BY Year) as prv_qty
FROM cte)

SELECT Subcat,
      qty_item,
      prv_qty,
      ROUND(((qty_item - prv_qty)/prv_qty),2) as YoY_rate
FROM prev_year
ORDER BY YoY_rate DESC
LIMIT 3;
--correct


/* Top 3 TerritoryID with biggest OrderQty of every year
Output: Year, TerritoryID, OrderQty, ranking
Group by Year, sort with largest order
DO not skip the rank > dense_rank
*/
WITH raw_data as(
SELECT FORMAT_DATE('%Y', c1.ModifiedDate) as Year
      ,c2.TerritoryID
      ,SUM(c1.OrderQty) as Q_of_items
FROM `adventureworks2019.Sales.SalesOrderDetail` as c1
  LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` as c2
    USING(SalesOrderID)
GROUP BY FORMAT_DATE('%Y', c1.ModifiedDate)
        ,c2.TerritoryID)
,
rank as(
SELECT *
      ,DENSE_RANK() OVER(PARTITION BY Year ORDER BY Q_of_items DESC) as ranking
FROM cte)

SELECT *
FROM raw_data
WHERE ranking <= 3
ORDER BY Year DESC, ranking;



/* Total discount cost belongs to Seasonal Discount for each Subcategory
Output: Year, Subcat name, Total cost for discount
Type: Seasonal Discount
Total discount cost = % Discount * OrderQty */

SELECT FORMAT_DATE('%Y', c1.ModifiedDate) as Year
      ,c4.Name
      ,(SUM(c1.OrderQty) * c1.UnitPrice * c2.DiscountPct) as Total_discount_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` as c1
  LEFT JOIN `adventureworks2019.Sales.SpecialOffer`  as c2
    USING(SpecialOfferID)
  LEFT JOIN `adventureworks2019.Production.Product` as c3
    USING(ProductID)
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS c4
    ON CAST(c3.ProductSubcategoryID AS INT) = c4.ProductSubcategoryID
WHERE c2.Type like '%Seasonal Discount%'
GROUP BY 
        FORMAT_DATE('%Y', c1.ModifiedDate)
        ,c4.Name
        ,c1.UnitPrice
        ,c2.DiscountPct;

--mình sẽ luôn tách aggregate function và field*field ra, cho dễ nhìn, dễ kiểm soát output thoi
select 
    FORMAT_TIMESTAMP("%Y", ModifiedDate), Name
    , sum(disc_cost) as total_cost
from (
      select distinct a.*
      , c.Name
      , d.DiscountPct, d.Type
      , a.OrderQty * d.DiscountPct * UnitPrice as disc_cost 
      from `adventureworks2019.Sales.SalesOrderDetail` a
      LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID
      LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d on a.SpecialOfferID = d.SpecialOfferID
      WHERE lower(d.Type) like '%seasonal discount%' 
)
group by 1,2
;

--query 5

WITH raw_data as(
SELECT
      EXTRACT(month from ModifiedDate) as month_order,
      CustomerID,
      COUNT(DISTINCT SalesOrderID) as sales_cnt  
FROM `adventureworks2019.Sales.SalesOrderHeader`
WHERE EXTRACT(year from ModifiedDate) = 2014 AND Status = 5
GROUP BY 1,2
ORDER BY 1,2) --Find all the customers having succesful transactions in all months
,
rank as(
SELECT 
    month_order as month_join,
    CustomerID,
    DENSE_RANK() OVER(PARTITION BY CustomerID ORDER BY month_order) AS _rank
FROM raw_data
ORDER BY CustomerID) --Finding the first transaction of customer
,
first_order as(
SELECT *
FROM rank
where _rank = 1) --Finding new customers of months
,
month_mapping as(
SELECT
      t1.CustomerID,t1.month_order, t2.month_join
FROM cte1 as t1
left join cte3 as t2
using(CustomerID)
ORDER BY CustomerID) --Combine all customers with new customers
,
month_gap as(
SELECT customerID
      ,month_order
      ,month_join
      ,CONCAT('M - ', month_order - month_join) as month_diff
FROM month_mapping)

Select month_join, month_diff, count(distinct customerID) as customer_cnt
from month_gap
group by month_join, month_gap.month_diff
order by month_join, month_diff;




/* Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
Output: Product name, month and year, stock qty, previous stock, the diff of stock
Use LEAD or LAG (up to me)
Filter year = 2011 */
WITH data AS(
SELECT
      c2.Name AS pr_name,  
      EXTRACT(month from c1.ModifiedDate) AS month,
      EXTRACT(year from c1.ModifiedDate) AS year,
      SUM(c1.StockedQty) AS stock_qty
FROM `adventureworks2019.Production.WorkOrder` AS c1
  LEFT JOIN `adventureworks2019.Production.Product` AS c2
    USING(ProductID)
WHERE EXTRACT(year from c1.ModifiedDate) = 2011
GROUP BY 1,2,3
ORDER BY 1,2 DESC)

, data2 AS(
  SELECT 
      pr_name,
      month, stock_qty,
      LEAD(stock_qty) OVER(PARTITION BY pr_name ORDER BY month DESC) AS prev_qty
  FROM data
  ORDER BY pr_name)

SELECT
      *,
      coalesce(ROUND((stock_qty - prev_qty)*100.0/prev_qty,1),0) AS MoM_diff
FROM data2;

--bổ sung thêm hàm coalesce

/* Calc Ratio of Stock / Sales in 2011 by product name, by month
Order results by month desc, ratio desc. Round Ratio to 1 decimal
ratio = stock / sales
total orderqty = total_sales 
data mapping when join */
WITH 
sale_info as(
    SELECT EXTRACT(month FROM c1.ModifiedDate) as month
          ,EXTRACT(year FROM c1.ModifiedDate) as year
          ,c1.ProductID
          ,c2.Name
          ,SUM(c1.OrderQty) as sales
    FROM `adventureworks2019.Sales.SalesOrderDetail` as c1
      LEFT JOIN `adventureworks2019.Production.Product` as c2
        USING(ProductID)
    WHERE EXTRACT(year FROM c1.ModifiedDate) = 2011
    GROUP BY EXTRACT(month FROM c1.ModifiedDate)
          ,EXTRACT(year FROM c1.ModifiedDate)
          ,c1.ProductID
          ,c2.Name)
,
stock_info as(
SELECT EXTRACT(month FROM ModifiedDate) as month
      ,EXTRACT(year FROM ModifiedDate) as year
      ,ProductID
      ,SUM(StockedQty) as stock
FROM `adventureworks2019.Production.WorkOrder`
WHERE EXTRACT(year FROM ModifiedDate) = 2011
GROUP BY EXTRACT(month FROM ModifiedDate)
        ,EXTRACT(year FROM ModifiedDate)
        ,ProductID)
,
mapping as(
SELECT cte.month
      ,cte.year
      ,cte.ProductID
      ,cte.Name
      ,cte.sales
      ,cte2.stock
FROM sale_info 
  LEFT JOIN stock_info 
    USING(ProductID,month)
WHERE cte.sales IS NOT NULL and cte2.stock IS NOT NULL
ORDER BY month DESC)

SELECT *
      ,ROUND((stock/sales),1) as ratio
FROM mapping
ORDER BY month DESC
        ,ratio DESC;



/* No of order and value at Pending status in 2014
Output: year, status, order, value
Get data from PurchaseOrderHeader: modifieddate, purchaseorderid, totaldue, stattus
filter year = 2014; status = pending (or number 1) */

SELECT EXTRACT(year FROM ModifiedDate) as year
      ,Status
      ,COUNT(PurchaseOrderID) as order_cnt
      ,ROUND(SUM(TotalDue),2) as value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE Status = 1
    and EXTRACT(year FROM ModifiedDate) = 2014
GROUP BY EXTRACT(year FROM ModifiedDate)
        ,Status;


                                                        --very good---