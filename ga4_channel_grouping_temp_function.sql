create temp function ChannelGrouping(csource string,medium string,campaign string,gclid string) as (
    case
        when medium='organic' then 'Organic Search'
        when (csource = 'direct' or csource is null) 
            and (regexp_contains(medium, r'^(\(not set\)|\(none\))$') or medium is null) 
            then 'Direct'
        when regexp_contains(csource, r'^(google|bing)$') 
            and regexp_contains(medium, r'^(cp.*|ppc)$') or gclid is not null
            then 'SEA'
        when  medium='psm' or regexp_contains(csource,r'mydealz') 
            then 'Price Comparison'
        when  medium='social' then 'Social Paid'
        when  medium='post' then 'Social'
        when regexp_contains(medium,r'display|video') 
            then 'Display'
        when medium='email' or (regexp_contains(csource,r'.*mail\.|.*deref\-')) then 'CRM'
        when  regexp_contains(campaign,r'rmkt')  
            or (regexp_contains(medium,r'rmkt')) then 'Remarketing'
        when  regexp_contains(medium,r'[Cc]oop') then 'Cooperation'
        when  medium='voucher' 
            then 'Voucher'
        when medium='referral' 
            then 'Referral'
        when  medium='affiliate' then 'Affiliate'
        else '(Other)'
    end
);
