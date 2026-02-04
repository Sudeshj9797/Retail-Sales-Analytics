# Retail Sales & Warranty Analytics Dashboard

## Project Overview
This project is an **end-to-end Retail Sales Analytics solution** built to simulate a real-world company analytics workflow.  
The goal is to analyze **sales performance, product contribution, store performance, and warranty quality issues** using structured data modeling and interactive dashboards.

The project demonstrates the complete analytics lifecycle:
**Raw data → Data cleaning → ETL → MySQL → SQL analysis → Power BI dashboard → Business insights**

---

## Business Objectives
- Analyze overall sales performance and revenue trends
- Identify top-performing products and stores
- Measure sales efficiency using KPIs
- Monitor warranty claims to evaluate product quality
- Provide actionable insights to support business decisions

---

## Tools & Technologies
- **Python** (Pandas, NumPy) – data cleaning and transformation  
- **MySQL** – relational database and star schema modeling  
- **SQL** – business analysis and KPI queries  
- **Power BI** – interactive dashboard and visualization  
- **CSV / Excel** – raw data sources  

---

## Data Pipeline (ETL Workflow)
1. Raw CSV files containing messy retail data
2. Data cleaning using Python:
   - Handled missing values
   - Removed duplicates
   - Standardized date formats
   - Created derived columns (revenue, year, month)
3. Loaded cleaned data into MySQL using ETL scripts
4. Designed a star/snowflake schema
5. Performed SQL-based business analysis
6. Built an interactive Power BI dashboard connected to MySQL

---

## Data Model
### Fact Tables
- **fact_sales** – transactional sales data
- **fact_warranty** – post-sale warranty claims

### Dimension Tables
- **dim_date**
- **dim_products**
- **dim_stores**
- **dim_category**

The model follows a **star/snowflake schema** optimized for analytics and BI reporting.

---

## Key KPIs
- **Total Revenue**
- **Total Orders**
- **Average Order Value (AOV)**
- **Warranty Rate (%)**

---

## Dashboard Features
- Monthly revenue trend analysis
- Top 10 products by revenue
- Store-wise revenue performance comparison
- Warranty claims trend over time
- Products with highest warranty rates
- Interactive slicers for:
  - Year
  - Month
  - Store
  - Product

---

## Key Business Insights
- Revenue shows clear seasonality with strong peaks in specific months.
- A small group of products contributes a majority of total revenue.
- Store performance varies significantly, indicating optimization opportunities.
- Warranty claims increase during high-sales periods.
- Some high-revenue products also exhibit higher warranty rates, highlighting quality risks.

---

## Business Recommendations
- Focus marketing and inventory planning on top-performing products.
- Investigate products with high warranty rates to improve quality.
- Replicate best practices from high-performing stores.
- Prepare inventory and staffing in advance for peak sales periods.

---

## Conclusion
This project demonstrates strong **data analytics fundamentals**, including ETL, SQL-based analysis, data modeling, and business-focused dashboarding.  
It reflects how analytics is performed in real companies to support decision-making.

---
