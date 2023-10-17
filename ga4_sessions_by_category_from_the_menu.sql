/*
You need to parse categories in the menu first. Via VBA for example.

Function URL(Hyperlink As Range)
  URL = Hyperlink.Hyperlinks(1).Address
End Function

1. Copy names "as is" from the menu
2. Extract from in Excel
3. Map in BQ

*/



with final as (
with date_range as (
  select
    '20231001' as start_date,
    '20231015' as end_date),
menu as (select
        event_date
       ,replace(split((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/')[safe_offset(2)],'www.','') as shop_name
       ,user_pseudo_id
       ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
       ,split(substr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,
             instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/',1,3)),'?')[safe_offset(0)] as pagePath
            from
      `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
      where 
      _TABLE_SUFFIX between start_date and end_date and event_name = 'page_view')
select case when pagePath='/fahrraeder/e-bikes/' then 'E-Bikes'
when pagePath='/sale/fahrraeder/e-bikes/' then 'Sale %'
when pagePath='/fahrraeder/e-bikes/e-bikes-trekking/' then 'E-Bikes Trekking'
when pagePath='/fahrraeder/e-bikes/e-mountainbikes/' then 'E-Mountainbikes'
when pagePath='/fahrraeder/e-bikes/e-bikes-city/' then 'E-Bikes City'
when pagePath='/fahrraeder/e-bikes/lastenfahrraeder/' then 'E-Lastenfahrräder'
when pagePath='/fahrraeder/e-bikes/e-bikes-rennrad/' then 'E-Bikes Rennrad'
when pagePath='/fahrraeder/e-bikes/e-bikes-urban/' then 'E-Bikes Urban'
when pagePath='/teile/e-bike-teile/' then 'E-Bike Teile'
when pagePath='/teile/e-bike-teile/e-bike-akkus-ladegeraete/' then 'Akkus'
when pagePath='/teile/e-bike-teile/e-bike-reifen/' then 'Reifen'
when pagePath='/teile/e-bike-teile/e-bike-bremsen/' then 'Bremsen'
when pagePath='/teile/e-bike-teile/e-bike-beleuchtung/' then 'Beleuchtung'
when pagePath='/teile/e-bike-teile/e-bike-ketten/' then 'Ketten'
when pagePath='/teile/e-bike-teile/e-bike-kettenblaetter/' then 'Kettenblätter'
when pagePath='/teile/e-bike-teile/e-bike-laufraeder/' then 'Laufräder'
when pagePath='/ratgeber/e-bike/10100.html' then 'E-Bike Kaufberatung'
when pagePath='/ratgeber/e-bike/typen/10101.html' then 'Welches E-Bike ist das richtige für dich?'
when pagePath='/ratgeber/e-bike/akkus/10103.html' then 'Alles über E-Bike Akkus'
when pagePath='/ratgeber/e-bike/ladestationen/10112.html' then 'Akku Ladestationen'
when pagePath='/e-bike-marken/3013.html' then 'Entdecke unsere E-Bike-Marken'
when pagePath='/stationaerer-aufbauservice.html' then 'Unser Aufbauservice'
when pagePath='/bikeleasing/' then 'E-Bike Leasing'
else null end as e_bikes
,case when pagePath='/fahrraeder/mountainbikes/' then 'Mountainbikes'
when pagePath='/fahrraeder/mountainbikes/mtb-hardtails/' then 'MTB Hardtails'
when pagePath='/fahrraeder/mountainbikes/mtb-fullys/' then 'MTB Fullys'
when pagePath='/fahrraeder/mountainbikes/mtb-29-zoll/' then 'MTB 29 Zoll'
when pagePath='/fahrraeder/mountainbikes/mtb-275-zoll-650b/' then 'MTB 27,5 Zoll (650B)'
when pagePath='/fahrraeder/rennraeder/' then 'Rennräder'
when pagePath='/fahrraeder/rennraeder/gravel-bikes/' then 'Gravel Bikes'
when pagePath='/fahrraeder/rennraeder/cyclocross-bikes/' then 'Cyclocross Bikes'
when pagePath='/fahrraeder/rennraeder/aero-rennraeder/' then 'Aero Rennräder'
when pagePath='/fahrraeder/e-bikes/' then 'E-Bikes'
when pagePath='/fahrraeder/e-bikes/e-mountainbikes/' then 'E-Mountainbikes'
when pagePath='/fahrraeder/e-bikes/e-bikes-trekking/' then 'E-Bikes Trekking'
when pagePath='/fahrraeder/e-bikes/suv-e-bikes/' then 'SUV E-Bikes'
when pagePath='/fahrraeder/e-bikes/e-bikes-city/' then 'E-Bikes City'
when pagePath='/fahrraeder/trekkingraeder/' then 'Trekkingräder'
when pagePath='/fahrraeder/trekkingraeder/trekkingraeder-herren/' then 'Trekkingräder Herren'
when pagePath='/fahrraeder/trekkingraeder/trekkingraeder-damen/' then 'Trekkingräder Damen'
when pagePath='/fahrraeder/trekkingraeder/crossraeder/' then 'Crossräder'
when pagePath='/fahrraeder/trekkingraeder/reiseraeder/' then 'Reiseräder'
when pagePath='/fahrraeder/cityraeder/' then 'Cityräder'
when pagePath='/fahrraeder/cityraeder/city-damenraeder/' then 'City Damenräder'
when pagePath='/fahrraeder/cityraeder/city-herrenraeder/' then 'City Herrenräder'
when pagePath='/fahrraeder/cityraeder/hollandraeder/' then 'Hollandräder'
when pagePath='/fahrraeder/jugend-kinderfahrraeder/' then 'Jugend- & Kinderfahrräder'
when pagePath='/fahrraeder/jugend--kinderfahrraeder/kinderraeder/' then 'Kinderräder'
when pagePath='/fahrraeder/jugend--kinderfahrraeder/jugendraeder/' then 'Jugendräder'
when pagePath='/fahrraeder/jugend--kinderfahrraeder/lauflernraeder/' then 'Lauflernräder'
when pagePath='/ratgeber/mountainbike/10200.html' then 'MTB Berater'
when pagePath='/ratgeber/rahmengroessen/10800.html' then 'Rahmenhöhe berechnen'
when pagePath='/ratgeber/trekkingrad/10500.html' then 'Trekking Berater'
when pagePath='/ratgeber/10000.html' then 'alle Berater'
when pagePath='/sale/fahrraeder/2.-wahl-fahrraeder/' then 'Fahrräder B-Ware'
else null end as fahrraeder
,count(distinct ga_session_id) as sessions
from menu
group by 1,2)
select e_bikes
,fahrraeder
,case when e_bikes is not null then 'E-Bikes'
      when fahrraeder is not null then 'Fahrräder' else null end as menu
,sessions
from final
where e_bikes is not null or fahrraeder is not null                   
order by 4 desc
