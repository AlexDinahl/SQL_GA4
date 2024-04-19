----Not finished----Just basic ideas etc.

    
with steps as (
                            select
                                          parse_date('%Y%m%d',date) as visit_date
                                          ,concat(fullvisitorid,'.',visitid) as sid
                                          ,hitNumber
                                          ,h.page.pagepath
                                          ,device.devicecategory as device
                                          ,if(h.eCommerceAction.action_type="6",5,if(h.eCommerceAction.action_type="3",-2,h.eCommerceAction.step)) as step
                                        from
                                          `bigquery.123456789.ga_sessions_*` as m, unnest (hits) h
                                          --inner join (select concat(fullvisitorid,'.',visitid) as sid from `bigquery.123456789.ga_sessions_*`, unnest (hits) h
                                          --where _table_suffix between '20230807' and '20230807' and h.eCommerceAction.action_type="3")
                                          --on concat(m.fullvisitorid,'.',m.visitid)=concat(fullvisitorid,'.',visitid)
                                        where _table_suffix between '20230807' and '20230807'
                                        and h.eCommerceAction.action_type in ("3","5","6")
                                        --and concat(fullvisitorid,'.',visitid)="1011605653348667862.1688195542"
                                        group by 1,2,3,4,5,6
                                        order by sid, hitNumber
 )
select step,count(distinct sid) from steps
group by step;
     
      
     
     
with final as (
                  with steps as (
                             select
                                          parse_date('%Y%m%d',date) as visit_date
                                          ,concat(fullvisitorid,'.',visitid) as sid
                                          ,hitNumber
                                          ,h.page.pagepath
                                          ,device.devicecategory as device
                                          ,if(h.eCommerceAction.action_type="6",5,if(h.eCommerceAction.action_type="3",-2,h.eCommerceAction.step)) as step
                                        from
                                          `bigquery.123456789.ga_sessions_*` as m, unnest (hits) h
                                        where _table_suffix between '20230807' and '20230807'
                                        --and h.eCommerceAction.action_type in ("3","5","6")
                                        group by 1,2,3,4,5,6
                                        order by sid, hitNumber
                  )
      select *
      
      ,if(step=1 and first_value(if(step=-1,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_1_without_step_cart
      ,if(step=2 and first_value(if(step=1,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_2_without_step_1
      ,if(step=3 and first_value(if(step=2,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_3_without_step_2
      ,if(step=4 and first_value(if(step=3,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_4_without_step_3
      ,if(step=5 and first_value(if(step=4,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_5_without_step_4
      ,if(step=-1 and lead(step) over (partition by sid order by step) is null,1,null) as cart_abandonment
      ,if(step=1 and lead(step) over (partition by sid order by step) is null,1,null) as step_1_abandonment
      ,if(step=2 and lead(step) over (partition by sid order by step) is null,1,null) as step_2_abandonment
      ,if(step=3 and lead(step) over (partition by sid order by step) is null,1,null) as step_3_abandonment
      ,if(step=4 and lead(step) over (partition by sid order by step) is null,1,null) as step_4_abandonment
      ,if(lag(pagepath) over (partition by sid order by step) in ("/warenkorb/") and pagepath="/lieferung/",1,0) as step_2_from_cart
      ,if(lag(pagepath) over (partition by sid order by step) in ("/warenkorb/") and pagepath="/bestellzusammenfassung/",1,0) as step_4_from_cart
      ,if(lag(pagepath) over (partition by sid order by step) in ("/bestellen/") and pagepath="/warenkorb-zusammengeführt/",1,0) as step_merged_cart_from_step_2
      from steps
      )
select count(distinct case when step_merged_cart_from_step_2=1 then sid else null end)
--step,sum(if(step=-1,1,0)) as checkout
--,sum(step_2_from_cart)
--,sum(step_4_from_cart)
--step,count(distinct sid) as sessions
/*
,sum(if(cart_abandonment is not null,1
                        ,if(step_1_abandonment is not null,1,
                        if(step_2_abandonment is not null,1,
                       if(step_3_abandonment is not null,1,
                     0))))) as exits
*/  
from final
--where step=-1
--where sid in (select distinct sid from final where step_4_without_step_3=1)
--group by 1,2
--order by sid,step
  ;  
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
with final as (
                  with steps as (
                              select
                                          parse_date('%Y%m%d',date) as visit_date
                                          ,concat(fullvisitorid,'.',visitid) as sid
                                          ,h.page.pagepath
                                          ,device.devicecategory as device
                                          ,if(h.eCommerceAction.action_type="6",5,h.eCommerceAction.step) as step
                                        from
                                          `bigquery.123456789.ga_sessions_*`, unnest (hits) h
                                        where _table_suffix between '20230807' and '20230807'
                                        and h.eCommerceAction.action_type in ("5","6")
                                        --and concat(fullvisitorid,'.',visitid)="1011605653348667862.1688195542"
                                        group by 1,2,3,4,5
                                        order by sid, step
                  )
      select *
      ,if(step=1 and first_value(if(step=-1,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_1_without_step_cart
      ,if(step=2 and first_value(if(step=1,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_2_without_step_1
      ,if(step=3 and first_value(if(step=2,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_3_without_step_2
      ,if(step=4 and first_value(if(step=3,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_4_without_step_3
      ,if(step=5 and first_value(if(step=4,1,null) ignore nulls) over (partition by sid order by step) is null,1,null) as step_5_without_step_4
      ,if(step=-1 and lead(step) over (partition by sid order by step) is null,1,null) as cart_abandonment
      ,if(step=1 and lead(step) over (partition by sid order by step) is null,1,null) as step_1_abandonment
      ,if(step=2 and lead(step) over (partition by sid order by step) is null,1,null) as step_2_abandonment
      ,if(step=3 and lead(step) over (partition by sid order by step) is null,1,null) as step_3_abandonment
      ,if(step=4 and lead(step) over (partition by sid order by step) is null,1,null) as step_4_abandonment
      ,if(lag(pagepath) over (partition by sid order by step) in ("/warenkorb/")
      and first_value(if(step=1,1,null) ignore nulls) over (partition by sid order by step) is null,1,0) as step_2_from_cart
      from steps)
select visit_date,sid,pagepath,step,step_2_without_step_1,step_2_from_cart
--step,count(distinct sid) as sessions
/*
,sum(if(cart_abandonment is not null,1
                        ,if(step_1_abandonment is not null,1,
                        if(step_2_abandonment is not null,1,
                       if(step_3_abandonment is not null,1,
                     0))))) as exits
*/  
from final
--where step=-1
where sid in (select distinct sid from final where step_2_without_step_1=1)
group by 1,2,3,4,5,6
order by sid,step;










################################################First var not corret#################################################
/*
with total as (
with date_range as (
  select
    '20230807' as start_date,
    --format_date('%y%m%d',date_sub(current_date(), interval 1 day))  
    '20230807' as end_date),
final_checkout as (
with checkout as ( 

   select
                                          parse_date('%Y%m%d',date) as visit_date
                                          ,concat(fullvisitorid,'.',visitid) as sid
                                          ,h.page.pagepath
                                          ,device.devicecategory as device
                                          --,concat(h.page.pagePath,h.page.pageTitle,fullVisitorId,visitid) as unique_pageviews
                                          --,h.hitnumber as hit_number
                                          
                                          --,if(h.page.pagepath like "%/warenkorb/%" and h.eCommerceAction.step=-1,1,0) as cart_sessions
                                          --,if(h.page.pagepath like "%/bestellen/%" and h.eCommerceAction.step=1,1,0) as register_sessions
                                          --,if(h.page.pagepath like "%/lieferung/%" and h.eCommerceAction.step=2,1,0) as delivery_sessions
                                          --,if(h.page.pagepath like "%/zahlart/%" and h.eCommerceAction.step=3,1,0) as payment_sessions
                                          --,if(h.page.pagepath like "%/bestellzusammenfassung/%" and h.eCommerceAction.step=4,1,0) as summary_sessions
                                        
                                          ,case 
                                                when h.eCommerceAction.step=-1 then "1_cart"
                                                when h.eCommerceAction.step=1 then "2_sign_in" 
                                                when h.eCommerceAction.step=2 then "3_delivery" 
                                                when h.eCommerceAction.step=3 then "4_payment"
                                                when h.eCommerceAction.step=4 then "5_summary" 
                                                when h.eCommerceAction.action_type="6" then "6_transactions"
                                          else null end
                                          as checkout_step
                                          ,case 
                                                when h.eCommerceAction.step=-1 then 1
                                                when h.eCommerceAction.step=1 then 2 
                                                when h.eCommerceAction.step=2 then 3 
                                                when h.eCommerceAction.step=3 then 4
                                                when h.eCommerceAction.step=4 then 5 
                                                when h.eCommerceAction.action_type="6" then 6
                                          else null end
                                          as checkout_step_index
                                        from
                                          `bigquery.123456789.ga_sessions_*`,date_range, unnest (hits) h
                                        where _table_suffix between start_date and end_date
                                        and h.eCommerceAction.action_type in ("5","6")
                                        --and concat(fullvisitorid,'.',visitid)="1011605653348667862.1688195542"
                                        group by 1,2,3,4,5,6
)
select * except (device), lead(checkout_step_index) over (partition by sid order by checkout_step_index) as ld
--visit_date,checkout_step, count(distinct sid) as sessions
from checkout
where checkout_step is not null
--group by 1,2
order by sid,checkout_step)
select * --,sum(if(((if(ld is null ,0,ld)-checkout_step_index)=1),0,1)) as exits 
,sum(if(checkout_step_index<>6 and (if(ld is null ,0,ld)-checkout_step_index)<>1,1,0)) as exits
from final_checkout
group by 1,2,3,4,5,6
order by sid,checkout_step_index
)
select visit_date,checkout_step,count(distinct sid) as sessions, sum(exits) as exits
from total
group by 1,2
order by 2
*/
