use Ecommerce_database;

-- Customer Analytics-

--1. Identify top 10 customers by total revenue and their purchase frequency.

select customerid, full_name, sum(total_order_amount) as total_spending, avg(convert(float,datediff(day,previous_order_date,orderdate)))
as 'Order_frequency i.e, one order per n days'  from
(select a.customerid,FirstName+' '+LastName as full_name, orderid,total_order_amount,orderdate, lag(orderdate,1,orderdate) over(partition by a.customerid
order by orderdate) as previous_order_date from customers a join orders b on a.customerid=b.customerid) as zxc
where datediff(day,previous_order_date,orderdate)>=1 group by customerid,full_name;


--2. Segment customers into tiers (e.g., tier 1 for high-value, tier 2 for medium-value, tier 3 for low-value) based on total spending and then calculate total number of customers in those tiers and average delivery time for those tiers.

with cte as
(Select a.Customerid, avg(convert(float,datediff(day,orderdate,DeliveryDate))) as avg_delivery_time, sum(total_order_amount) as total_spending
from customers a join orders b on a.customerid=b.customerid group by a.Customerid),
cte2 as
(select *, NTILE(3) over (order by total_spending desc) as tier from cte)
select tier, count(customerid) as total_customers,avg(avg_delivery_time) as average_delivery_time from cte2 group by tier;


--3. Rank customers on the basis of average time between orders and also find average delivery time for them.

with cte as
(select customerid, full_name,avg(convert(float,datediff(day,orderdate,DeliveryDate))) as avg_delivery_time , avg(convert(float,datediff(day,previous_order_date,orderdate)))
as avg_days_per_order  from
(select a.customerid,FirstName+' '+LastName as full_name, DeliveryDate,orderdate, lag(orderdate,1,orderdate) over(partition by a.customerid
order by orderdate) as previous_order_date from customers a join orders b on a.customerid=b.customerid) as zxc
where datediff(day,previous_order_date,orderdate)>=1 group by customerid,full_name)
select customerid,full_name,avg_days_per_order,avg_delivery_time,rank() over(order by avg_days_per_order asc) as ranking
from cte;


--4. Rank customers by avg order size (quantity) across each country.

select *,rank() over(order by avg_order_size desc)as rnk from (select a.customerid,FirstName+' '+LastName as full_name,Country, round(avg(order_size),2) as avg_order_size
from customers a join Orders b on a.customerid=b.customerid join (select orderid,convert(float,sum(quantity)) as order_size from orderdetails group by orderid)as c
on b.orderid=c.orderid group by a.customerid,FirstName+' '+LastName,Country) as zxc;


--5. Analyze how customer spending varies by location or region.

select country, state, round(avg(total_order_amount),2) as average_total_spending from customers a join Orders b on a.customerid=b.customerid
group by country,state order by country,state;


--Orders Analysis-

--6. Calculate the average order value (AOV) and track changes over time.

select orderid,Total_order_amount,OrderDate, AVG(total_order_amount) over(order by orderdate) as Avg_Cummulative
from customers a join orders b on a.customerid=b.customerid;

--7. Analyze the distribution of order quantities over time.

select b.Orderid,orderdate,order_size, round(Avg(order_size) over(order by orderdate),2) as Cummulative_Average from customers a join orders b
on a.customerid=b.customerid join (select orderid,convert(float,sum(quantity)) as order_size from orderdetails group by orderid) as c on b.orderid=c.OrderID;

--8. Identify orders with unusual characteristics (e.g., 25 orders having with least order amount & 25 orders with max order amount)

select orderid,orderdate,total_order_amount from (select top 25 orderid,orderdate,total_order_amount, rank() over(order by total_order_amount) as rnk from customers a join orders b
on a.customerid=b.customerid
union
select top 25 orderid,orderdate,total_order_amount, rank() over(order by total_order_amount desc) as rnk from customers a join orders b
on a.customerid=b.customerid) as zxc order by Total_order_amount;

--9. Get a list of number of orders monthly over years and total sales amount i.e, revenue of each month.

select year(orderdate) as Yr,month(orderdate) as mnth,round(sum(total_order_amount),0) as total_sales from customers a join orders b on a.customerid=b.customerid
group by year(orderdate),month(orderdate) order by Yr,mnth ;

--10. Calculate performance improvement each month compared to average of previous 3 months (number of orders).

with cte as
(select year(orderdate) as Yr,month(orderdate) as mnth,convert(float,count(orderid)) as total_orders from customers a join orders b on a.customerid=b.customerid
group by year(orderdate),month(orderdate)),
cte2 as
(select Yr,mnth,total_orders,round(avg(total_orders) over(order by Yr,mnth rows between 3 preceding and 1 preceding),2) as Avgg_of_prev_3_mnths from cte)
select Yr as 'Year',mnth as 'Month',total_orders, Avgg_of_prev_3_mnths,round((total_orders-Avgg_of_prev_3_mnths)*100/avgg_of_prev_3_mnths,2)
as performance_improvement from cte2;

--Product Analysis-

--11. Identify best-selling products based on total revenue and units sold.

select Product, round(sum(total_order_amount),2) as total_sales, sum(quantity) as Units_sold from orders a join OrderDetails b on a.OrderID=b.OrderID
join Products c on b.productid=c.productid group by Product order by total_sales desc,Units_sold;

--12. Analyze revenue contribution by product sub- category.

select Sub_Category, total_sales*100/(select sum(total_order_amount) from orders) as revenue_percent from (select Sub_Category, round(sum(total_order_amount),2) as total_sales
from orders a join OrderDetails b on a.OrderID=b.OrderID join Products c on b.productid=c.productid group by Sub_Category) as zxc
order by total_sales desc;

--13. Identify Increase in demand of most sold sub_category by time i.e, month.

--identifying most sold sub category as per quantity
select top 1 Sub_Category, round(sum(Quantity),2) as total_units from orders a join OrderDetails b on a.OrderID=b.OrderID
join Products c on b.productid=c.productid group by Sub_Category order by total_units desc;

-- identifying units sold for most sold sub_category monthwise-
select year(orderdate) as 'Year', month(orderdate) as 'Month', sum(quantity) as total_units_sold from orders b join orderdetails c on b.orderid=c.orderid
join products d on c.productid=d.productid where sub_category='Skin Care' group by year(orderdate),month(orderdate) order by 'Year','Month';

--14. Identify  least selling Products on the basis of quantity that takes maximum time to deliver.

select product, sum(quantity) as units_sold, avg(datediff(day,orderdate,DeliveryDate)) as ship_days from orders b join orderdetails c on b.orderid=c.orderid
join products d on c.productid=d.productid group by product order by units_sold,ship_days desc;

--15. Find which sub_category products was maximum purchased in top 3 cities across top 3 countries as per total sales.

with cte as
(select city,sub_category,sum(quantity) as units_sold from customers a join orders b on a.customerid=b.customerid join
orderdetails c on b.orderid=c.orderid join products d on c.productid=d.productid group by city,sub_category),
cte2 as
(select country,city, sum(total_order_amount) as city_sales from customers a join orders b on a.customerid=b.CustomerID group by country,city),
cte3 as
(select country, sum(total_order_amount) as country_sales from customers a join orders b on a.customerid=b.CustomerID group by country),
cte4 as
(select *,rank() over(partition by country order by city_sales desc) as city_rank from cte2),
cte5 as
(select *,rank() over(order by country_sales desc) as country_rank from cte3),
cte6 as
(select city,sub_category,units_sold,rank() over(partition by city order by units_sold desc) as units_rank from cte)
select cte5.country,cte4.city,sub_category,units_sold from cte5 join cte4 on cte5.country=cte4.country join cte6 on cte4.city=cte6.city
where country_rank in(1,2,3) and city_rank in(1,2,3) and units_rank=1 order by country,city,units_sold desc;

--Region Wise Analysis

--16. Find the number of customers across each country and the number of orders across each country as well.

select country,count(distinct(a.customerid)) as num_of_customers,count(distinct(orderid)) as num_of_orders
from customers a join orders b on a.customerid=b.customerid
group by country order by num_of_orders desc;

--17. Get me top 10 states with highest average order amount and get me average quantity per order from that state.

select state, avg(total_order_amount) as per_order_amount, avg(t_quantity) as quantity_per_order from customers a join orders b on a.customerid=b.customerid
join (select orderid,sum(quantity) as t_quantity from orderdetails group by orderid) as c on b.orderid=c.orderid 
group by state order by per_order_amount desc;

--18. Get me top 3 states with least average delivery time and 3 with most average delivery time.

with cte as
(select state,round(avg(convert(float,(datediff(day,orderdate,shipdate)))),2) as average_delivery_time from customers a join orders b on a.customerid=b.customerid
group by state)
select top 3 *,dense_rank() over(order by average_delivery_time desc) as rnk,'Most delivery time' as tier from cte
union
select top 3 *,dense_rank() over(order by average_delivery_time) as rnk,'Least delivery time' from cte 

--19. 2. Get me a list of top 10 cities as per total revenue and find number of customers & orders from that city along with that particular city top customer (on the basis of highest order amount) customer id and full name from that city with his total spending.

with cte as
(select city, round (sum(total_order_amount), 2) as Total_Revenue, count(distinct (a.Customerid)) as Num_of_Customers,count (distinct(b.orderid))
as Num_of_Orders from customers a join orders b on a.customerid=b.customerid
group by city),
cte2 as
(select a.customerid, a.FirstName, a.LastName, City, total_order_amount, max(total_order_amount) over (partition by city) as max_order_amount
from customers a join orders b on a.customerid=b.customerid),
cte3 as
(select a.customerid, sum(total_order_amount) as total_spending from customers a join orders b on a.customerid=b.customerid
group by a.customerid),
cte4 as
(select customerid, FirstName, LastName, City, max_order_amount from cte2 where total_order_amount=max_order_amount),
cte5 as
(select a.city, Total_Revenue, Num_of_Customers, Num_of_Orders, customerid, firstname, lastname, max_order_amount
from cte a join cte4 b on a.city=b.city)
select top 10 city, Total_Revenue, Num_of_Customers, Num_of_Orders, a.customerid, firstname, lastname, total_spending from cte5 a join cte3 b
on a.customerid=b.customerid order by Total_Revenue desc;

--Payment & Shippers Analysis-

--20. Analyze payment method preferences (e.g., credit card, wallet, COD).

select paymenttype,count(distinct(orderid)) as count_of_payments from orders a join payments b on a.paymentid=b.paymentid group by paymenttype;






