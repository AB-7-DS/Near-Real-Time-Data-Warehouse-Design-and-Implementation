

USE Walmart_DW;

-- ============================================================
-- SECTION 1: PRODUCT PERFORMANCE & REVENUE ANALYSIS
-- ============================================================

-- Q1. Top Revenue-Generating Products (Weekdays vs Weekends, Monthly Drill-Down)
SELECT 
    p.Product_Category,
    t.Month,
    CASE WHEN t.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY p.Product_Category, t.Month, DayType
ORDER BY t.Month, Total_Revenue DESC
LIMIT 5;

-- Q2. Monthly Sales Growth by Product Category
SELECT 
    p.Product_Category,
    t.Month,
    SUM(f.Revenue) AS Total_Revenue,
    LAG(SUM(f.Revenue)) OVER (PARTITION BY p.Product_Category ORDER BY t.Month) AS Prev_Month_Revenue,
    ROUND(
        (SUM(f.Revenue) - LAG(SUM(f.Revenue)) OVER (PARTITION BY p.Product_Category ORDER BY t.Month))
        / LAG(SUM(f.Revenue)) OVER (PARTITION BY p.Product_Category ORDER BY t.Month) * 100, 2
    ) AS Growth_Percentage
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY p.Product_Category, t.Month
ORDER BY p.Product_Category, t.Month;

-- Q3. Product Category Sales by Occupation
SELECT 
    c.Occupation,
    p.Product_Category,
    SUM(f.Revenue) AS Total_Sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY c.Occupation, p.Product_Category
ORDER BY Total_Sales DESC;

-- Q4. Top 5 Products Purchased Together (Affinity Analysis)
SELECT 
    f1.Product_ID AS Product_A,
    f2.Product_ID AS Product_B,
    COUNT(*) AS Times_Bought_Together
FROM FactSales f1
JOIN FactSales f2 
  ON f1.Order_ID = f2.Order_ID AND f1.Product_ID <> f2.Product_ID
GROUP BY Product_A, Product_B
ORDER BY Times_Bought_Together DESC
LIMIT 5;

-- Q5. Seasonal Analysis of Product Sales (Dynamic Drill-Down)
SELECT 
    p.Product_Category,
    CASE 
        WHEN t.Month IN (12,1,2) THEN 'Winter'
        WHEN t.Month IN (3,4,5) THEN 'Spring'
        WHEN t.Month IN (6,7,8) THEN 'Summer'
        ELSE 'Fall' END AS Season,
    SUM(f.Revenue) AS Total_Sales
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY p.Product_Category, Season
ORDER BY p.Product_Category, Total_Sales DESC;

-- ============================================================
-- SECTION 2: CUSTOMER DEMOGRAPHICS & BEHAVIOR
-- ============================================================

-- Q6. Customer Demographics by Purchase Amount (City Breakdown)
SELECT 
    c.Gender,
    c.Age,
    c.City_Category,
    SUM(f.Revenue) AS Total_Purchase
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
GROUP BY c.Gender, c.Age, c.City_Category
ORDER BY Total_Purchase DESC;

-- Q7. Total Purchases by Gender and Age (Quarterly Trend)
SELECT 
    c.Gender,
    c.Age,
    t.Quarter,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY c.Gender, c.Age, t.Quarter
ORDER BY t.Quarter, Total_Revenue DESC;

-- Q8. Average Purchase Amount by Stay Duration and Gender
SELECT 
    c.Gender,
    c.Stay_In_Current_City_Years,
    AVG(f.Revenue) AS Avg_Purchase
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
GROUP BY c.Gender, c.Stay_In_Current_City_Years
ORDER BY Avg_Purchase DESC;

-- Q9. Weekend vs Weekday Sales by Age Group
SELECT 
    c.Age,
    CASE WHEN t.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS DayType,
    SUM(f.Revenue) AS Total_Sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY c.Age, DayType
ORDER BY Total_Sales DESC;

-- Q10. Top Occupations by Product Category Sales
SELECT 
    p.Product_Category,
    c.Occupation,
    SUM(f.Revenue) AS Revenue
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY p.Product_Category, c.Occupation
ORDER BY p.Product_Category, Revenue DESC
LIMIT 5;

-- ============================================================
-- SECTION 3: STORE, SUPPLIER & GEOGRAPHICAL ANALYSIS
-- ============================================================

-- Q11. Top 5 Revenue-Generating Cities by Product Category
SELECT 
    s.Store_Name,
    p.Product_Category,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimStore s ON f.Store_ID = s.Store_ID
GROUP BY s.Store_Name, p.Product_Category
ORDER BY Total_Revenue DESC
LIMIT 5;

-- Q12. Supplier Sales Contribution by Store and Product
SELECT 
    s.Store_Name,
    sp.Supplier_Name,
    p.Product_Category,
    SUM(f.Revenue) AS Total_Sales
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimSupplier sp ON f.Supplier_ID = sp.Supplier_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY s.Store_Name, sp.Supplier_Name, p.Product_Category
ORDER BY s.Store_Name, Total_Sales DESC;

-- Q13. City Category Performance by Marital Status (Monthly Breakdown)
SELECT 
    c.City_Category,
    c.Marital_Status,
    t.Month,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY c.City_Category, c.Marital_Status, t.Month
ORDER BY t.Month, Total_Revenue DESC;

-- Q14. Store Revenue Growth Rate Quarterly (2017 Example)
SELECT 
    s.Store_Name,
    t.Quarter,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
WHERE t.Year = 2017
GROUP BY s.Store_Name, t.Quarter
ORDER BY s.Store_Name, t.Quarter;

-- Q15. Revenue Volatility by Store and Supplier (Month-to-Month)
SELECT 
    s.Store_Name,
    sp.Supplier_Name,
    t.Month,
    SUM(f.Revenue) AS Monthly_Revenue,
    ROUND(
        (SUM(f.Revenue) - LAG(SUM(f.Revenue)) OVER (PARTITION BY s.Store_Name, sp.Supplier_Name ORDER BY t.Month))
        / LAG(SUM(f.Revenue)) OVER (PARTITION BY s.Store_Name, sp.Supplier_Name ORDER BY t.Month) * 100, 2
    ) AS Revenue_Volatility
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimSupplier sp ON f.Supplier_ID = sp.Supplier_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY s.Store_Name, sp.Supplier_Name, t.Month
ORDER BY Revenue_Volatility DESC;

-- ============================================================
-- SECTION 4: TREND, GROWTH & OUTLIER ANALYSIS
-- ============================================================

-- Q16. Quarterly Revenue Growth (ROLLUP Example)
SELECT 
    t.Year,
    t.Quarter,
    p.Product_Category,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY ROLLUP (t.Year, t.Quarter, p.Product_Category);

-- Q17. Yearly Revenue Trends by Store, Supplier, and Product (ROLLUP)
SELECT 
    s.Store_Name,
    sp.Supplier_Name,
    p.Product_Category,
    t.Year,
    SUM(f.Revenue) AS Total_Revenue
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimSupplier sp ON f.Supplier_ID = sp.Supplier_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY ROLLUP (s.Store_Name, sp.Supplier_Name, p.Product_Category, t.Year);

-- Q18. Revenue and Volume-Based Sales Analysis (H1 vs H2)
SELECT 
    p.Product_Category,
    CASE WHEN t.Month BETWEEN 1 AND 6 THEN 'H1' ELSE 'H2' END AS Half_Year,
    SUM(f.Revenue) AS Total_Revenue,
    SUM(f.Quantity) AS Total_Quantity
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY p.Product_Category, Half_Year
ORDER BY p.Product_Category, Half_Year;

-- Q19. Identify High Revenue Spikes in Product Sales (Outliers)
WITH daily_avg AS (
  SELECT 
    p.Product_Category,
    t.Date,
    SUM(f.Revenue) AS Daily_Sales,
    AVG(SUM(f.Revenue)) OVER (PARTITION BY p.Product_Category) AS Avg_Sales
  FROM FactSales f
  JOIN DimProduct p ON f.Product_ID = p.Product_ID
  JOIN DimTime t ON f.Time_ID = t.Time_ID
  GROUP BY p.Product_Category, t.Date
)
SELECT *
FROM daily_avg
WHERE Daily_Sales > 2 * Avg_Sales
ORDER BY Product_Category, Daily_Sales DESC;

-- Q20. Create View: STORE_QUARTERLY_SALES
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.Store_Name,
    t.Quarter,
    SUM(f.Revenue) AS Total_Sales
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimTime t ON f.Time_ID = t.Time_ID
GROUP BY s.Store_Name, t.Quarter
ORDER BY s.Store_Name, t.Quarter;
