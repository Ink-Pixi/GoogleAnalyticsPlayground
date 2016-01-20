SELECT  
	shop_orders_referenceno,
	count(distinct shop_orders_id) orders,
	SUM(so.shop_orders_cost_paid) Total
FROM 
	shop_orders so
WHERE 
	DATE(so.shop_orders_datetime) > '2015-12-21'
AND	
	shop_orders_transaction_status = 'Approved'
AND	
	(
			shop_orders_referenceno LIKE '1%E%'
		OR
			shop_orders_referenceno LIKE '-WC%'
		OR 
			shop_orders_referenceno = 'winter15'
	)
GROUP BY
	shop_orders_referenceno
	
	
	SELECT  
	shop_orders_referenceno,
	count(distinct shop_orders_id) orders,
	SUM(so.shop_orders_cost_paid) Total
FROM 
	shop_orders so
WHERE 
	DATE(so.shop_orders_datetime) > '2016-01-01'
AND	
	shop_orders_transaction_status = 'Approved'
AND	
	(
			shop_orders_referenceno LIKE '1%E%'
		OR
			shop_orders_referenceno LIKE '-WC%'
		OR 
			shop_orders_referenceno = 'winter15'
	)
GROUP BY
	shop_orders_referenceno

SELECT DISTINCT
		o.campaignID source_code,
		COUNT(o.orderNumber ) orders,
		SUM(o.GrandTotal) gross_revenue,
		'om' data_source
FROM 
		SQLSERVER.InkPixi.dbo.Orders o 
WHERE
		o.campaignID IS NOT NULL
AND
		o.campaignID != ''    
AND	
	(
			o.CampaignID LIKE '1%E%'
		OR
			o.CampaignID LIKE '-WC%'
		OR 
			o.CampaignID = 'winter15'
	)
AND
		o.orderDate > CONVERT(VARCHAR(12), '01/01/2016', 101)
GROUP BY
		o.campaignID

-- select * from shop_orders limit 1000


-- select distinct shop_orders_referenceno from shop_orders where shop_orders_referenceno != '' order by shop_orders_referenceno desc
-- select distinct shop_orders_referenceno from shop_orders where shop_orders_referenceno != '' and (shop_orders_referenceno like '16E%' or shop_orders_referenceno like '15E%' order by shop_orders_referenceno desc