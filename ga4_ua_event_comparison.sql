with date_range as (
  select
    '20231211' as start_date,
    '20231220' as end_date),
ua as (select distinct 
                 --h.page. pagePath
                 h.eventInfo.eventCategory
                ,h.eventInfo.eventAction
                ,h.eventInfo.eventLabel
                , case when  h.eventInfo.eventAction='Product Impression' then 'view_item_list'
                       when  h.eventInfo.eventAction='Promotion Impression' then 'view_promotion'
                       when  h.eventInfo.eventAction='Product Detail View' then 'view_item'
                       when  h.eventInfo.eventAction='Products In Cart' then 'items_in_cart'
                       when  h.eventInfo.eventAction='Product Click' then 'select_item'
                       when  h.eventInfo.eventAction='Promotion Click' then 'select_promotion'
                       when  h.eventInfo.eventAction='Add to Cart' then 'add_to_cart'
                       when  h.eventInfo.eventAction='Cart Page View' then 'view_cart'
                       when  h.eventInfo.eventAction='Checkout Step' and h.page.pagePath='/bestellen/' then 'begin_checkout'
                       when  h.eventInfo.eventAction='Checkout Step' and h.page.pagePath='/lieferung/' then 'add_shipping_info'
                       when  h.eventInfo.eventAction='Checkout Step' and h.page.pagePath='/zahlart/' then 'add_payment_info'
                       when  h.eventInfo.eventAction='Checkout Step' and h.page.pagePath='/bestellzusammenfassung/' then 'checkout_summary'
                       when  h.eventInfo.eventAction='Checkout Option' then 'checkout_option'
                       when  h.eventInfo.eventAction='Remove From Cart' then 'remove_from_cart'
                       when  h.eventInfo.eventAction='Purchase' then 'purchase'
                       when  h.eventInfo.eventAction='Order Cancel' then 'purchase_cancel'

                  --Non e-commerce events
                      when h.eventInfo.eventAction='Login' and h.eventInfo.eventLabel='Successfully' then'login_sucess'
                      when h.eventInfo.eventAction='Login' and h.eventInfo.eventLabel='Unsuccessfully' then'login_fail'
                      when h.eventInfo.eventAction='Search flyout' then'navigation_search_flyout'
                      when h.eventInfo.eventAction='Color Sibling' then'item_choose_color'
                      when h.eventInfo.eventAction='Choose size' then'item_choose_size'
                      when h.eventInfo.eventAction='Store submit' then'item_store_submit'
                      when h.eventInfo.eventAction='store_availability_select_store' then 'store_availability_select_store'
                      when h.eventInfo.eventAction='Frame size calculator' then'item_frame_size_calculator'
                      when h.eventInfo.eventAction='Read review' then'item_review_read'
                      when h.eventInfo.eventAction='Add' then'wishlist_add'
                      when h.eventInfo.eventAction='Remove' then'wishlist_remove'
                      when h.eventInfo.eventAction='Add to cart' then'wishlist_add_to_cart'
                      when h.eventInfo.eventAction='From cart to wishlist' then'wishlist_from_cart_to_wishlist'
                      when h.eventInfo.eventAction='Shipping options' then'add_shipping_info'
                      when h.eventInfo.eventAction='Payment options' then'add_payment_info'
                      when h.eventInfo.eventAction='Order Cancel' then'purchase_cancel'
                      when h.eventInfo.eventAction='Footer' then'navigation_footer'
                      when h.eventInfo.eventAction='Header' then'navigation_header'
                      when h.eventInfo.eventAction='Pagination' then'navigation_pagination'
                      when h.eventInfo.eventAction='Breadcrumb' then'navigation_breadcrumb'
                      when h.eventInfo.eventAction='Buffering' then'video_buffering'
                      when h.eventInfo.eventAction='Seek' then'video_seek'
                      when h.eventInfo.eventAction='Pause' then'video_pause'
                      when h.eventInfo.eventAction='Specifications show more' then'item_specifications_show'
                      when h.eventInfo.eventAction='Shipping Cost Table' then'item_shipping_cost_table'
                      when h.eventInfo.eventAction='Description show all models' then'item_show_all_models'
                      when h.eventInfo.eventAction='Store availability' then'item_store_availability'
                      when h.eventInfo.eventAction='Personal Delivery' then'item_personal_delivery'
                      when h.eventInfo.eventAction='Brand Logo' then'item_brand_logo_click'
                      when h.eventInfo.eventAction='Zoom' and h.eventInfo.eventLabel='Zoom in' then 'item_zoom_in'
                      when h.eventInfo.eventAction='Zoom' and h.eventInfo.eventLabel='Zoom out' then 'item_zoom_out'
                      when h.eventInfo.eventAction='Summary' and h.eventInfo.eventLabel='Edit shipping address' then'checkout_edit_shipping_address'
                      when h.eventInfo.eventAction='Summary' and h.eventInfo.eventLabel='Edit payment' then'checkout_edit_payment'
                      when h.eventInfo.eventAction='Submit review' then'reviews_submit_review'
                      when h.eventInfo.eventAction='Add review' then'reviews_add_review'
                      when h.eventInfo.eventAction='Registration submit' then'account_registration_submit'
                      when h.eventInfo.eventAction='Filter' and h.eventInfo.eventCategory='Navigation' then'navigation_filter'
                      when h.eventInfo.eventAction='Filter' and h.eventInfo.eventCategory='Reviews' then'reviews_filter'
                      when h.eventInfo.eventAction='Create account' then'sign_up'

                      when h.eventInfo.eventAction='Payback' and h.eventInfo.eventCategory='Cart' 
                          and regexp_contains(h.eventInfo.eventLabel, r'^[Ss]uccessfully') then 'payback_cart_success'
                      when h.eventInfo.eventAction='Payback' and h.eventInfo.eventCategory='Cart' 
                          and regexp_contains(h.eventInfo.eventLabel,r'^[Uu]nsuccessfully') then 'payback_cart_fail'
                      
                      when h.eventInfo.eventAction='Payback' and h.eventInfo.eventCategory='Summary' 
                          and regexp_contains(h.eventInfo.eventLabel, r'^[Ss]uccessfully') then 'payback_summary_success'
                      when h.eventInfo.eventAction='Payback' and h.eventInfo.eventCategory='Summary' 
                          and regexp_contains(h.eventInfo.eventLabel,r'^[Uu]nsuccessfully') then 'payback_summary_fail'

                      when h.eventInfo.eventAction='Voucher' and h.eventInfo.eventCategory='Cart' 
                          and regexp_contains(h.eventInfo.eventLabel, r'^[Ss]uccessfully') then 'cart_voucher_success'
                      when h.eventInfo.eventAction='Voucher' and h.eventInfo.eventCategory='Cart' 
                          and regexp_contains(h.eventInfo.eventLabel,r'^[Uu]nsuccessfully') then 'cart_voucher_fail'
                      
                      when h.eventInfo.eventAction='Voucher' and h.eventInfo.eventCategory='Summary' 
                          and regexp_contains(h.eventInfo.eventLabel, r'^[Ss]uccessfully') then 'summary_voucher_success'
                      when h.eventInfo.eventAction='Voucher' and h.eventInfo.eventCategory='Summary' 
                          and regexp_contains(h.eventInfo.eventLabel,r'^[Uu]nsuccessfully') then 'summary_voucher_fail'

                      when h.eventInfo.eventAction='Login' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel, r'^[Ss]uccessfully') then 'login_success'
                      when h.eventInfo.eventAction='Login' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel,r'^[Uu]nsuccessfully') then 'login_fail'

                      when h.eventInfo.eventAction='Newsletter' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel, r'Registration successfully') then 'newsletter_registration_success'
                      when h.eventInfo.eventAction='Newsletter' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel,r'Unsubscribe') then 'newsletter_unsubscribe'

                      when h.eventInfo.eventAction='Account' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel, r'Forgot password') then 'account_forgot_password'
                      when h.eventInfo.eventAction='Account' and h.eventInfo.eventCategory='Customer relations' 
                          and regexp_contains(h.eventInfo.eventLabel,r'Change password') then 'account_change_password'
                     when h.eventInfo.eventAction='Registration submit' then 'account_registration_submit'
                     when h.eventInfo.eventAction='availability_reminder' then 'availability_reminder'
                    
                      when h.eventInfo.eventCategory='Filter' then'filter'
                      when h.eventInfo.eventCategory='Filter' then'filter'
                      when h.eventInfo.eventCategory='Sorting' then'navigation_sorting'
                      when h.eventInfo.eventCategory like 'Smartfit%'  then'smartfit'
                      when h.eventInfo.eventCategory='Neocom Conversation' then'neocom_conversation'
                      when h.eventInfo.eventCategory='YouTube' and h.eventInfo.eventAction='Start playing' then'video_start'
                      when h.eventInfo.eventCategory='YouTube' and h.eventInfo.eventAction='Pause' then 'video_pause'
                      when h.eventInfo.eventCategory='YouTube' and h.eventInfo.eventAction='Reached the end' then 'video_complete'
                      when h.eventInfo.eventCategory='YouTube' and regexp_contains(h.eventInfo.eventAction,r'Reached...%') then 'video_progress'
                      when h.eventInfo.eventAction='store_availability_loaded' then'store_availability_loaded'
                      when h.eventInfo.eventAction='assembly_loaded' then'assembly_loaded'
                      when h.eventInfo.eventAction='pedal_info_link ATC_drawer' then'pedal_info_link_atc_drawer'
                      when h.eventInfo.eventAction='reviews_view_more' then'reviews_view_more'
                      when h.eventInfo.eventAction='pedal_info_link product_description' then'pedal_info_link_item_description'
                      when h.eventInfo.eventAction='testride_loaded' then'testride_loaded'
                      when h.eventInfo.eventAction='testride_open_form' then'testride_open_form'
                      when h.eventInfo.eventAction='klarna_ratenzahlung' then'item_klarna_ratenzahlung'
                      when h.eventInfo.eventAction='Cargo Bike Offer sent' then'cargo_bike_offer_sent'
                      when h.eventInfo.eventAction='Cargo Bike Offer more info open' then'cargo_bike_offer_more_info_open'
                      when h.eventInfo.eventAction='Cargo Bike Offer modal opened' then'cargo_bike_offer_modal_opened'
                      when h.eventInfo.eventAction='assembly_open' then'assembly_open'
                      when h.eventInfo.eventAction='assembly_close' then'assembly_close'
                      when h.eventInfo.eventAction='store_availability_select' then'store_availability_select'
                      when h.eventInfo.eventAction='Show More' then'reviews_show_more'
                      when h.eventInfo.eventAction='resultShown' then'smartfit_result_shown'
                      when h.eventInfo.eventAction='recommendationAvailable' then'smartfit_recommendation_available'
                      when h.eventInfo.eventAction='recommendationOpened' then'smartfit_recommendation_opened'
                      when h.eventInfo.eventAction='recommendationClicked' then'smartfit_recommendation_clicked'
                      when h.eventInfo.eventAction='opened' then'smartfit_opened'
                      when h.eventInfo.eventAction='armViewOpened' then'smartfit_arm_view_opened'
                      when h.eventInfo.eventAction='bikeFitted' then'smartfit_bike_fitted'
                      when h.eventInfo.eventAction='closed' then'smartfit_closed'
                      when h.eventInfo.eventAction='restarted' then'smartfit_restarted'
                      when h.eventInfo.eventAction='init_loaded' then'smartfit_init_loaded'
                      when h.eventInfo.eventAction='init_loadingFailed' then'smartfit_init_loading_failed'
                      when h.eventInfo.eventAction='heightViewOpened' then'smartfit_height_view_opened'
                      when h.eventInfo.eventAction='inseamViewOpened' then'smartfit_inseam_view_opened'
                      when h.eventInfo.eventAction='preferenceViewOpened' then'smartfit_preference_view_opened'
                      when h.eventInfo.eventAction='Open smartfit measure form' then'smartfit_open_measure_form'
                      when h.eventInfo.eventAction='Smartfit Measure form submit' then'smartfit_measure_form_submit'
                      when h.eventInfo.eventAction='store_availability_open_form' then'store_availability_open_form'
                      when h.eventInfo.eventAction='store_availability_close' then'store_availability_close'
                      when h.eventInfo.eventAction='availability_reminder' then'availability_reminder'
                      when  h.eventInfo.eventAction='item_image_spin' then 'item_image_spin'
                      when  h.eventInfo.eventAction='Product Series Link' then 'product_series_link'
                      when  h.eventInfo.eventAction='Size Variation Drawer' then 'size_variation_drawer'
                      when  h.eventInfo.eventAction='Geometry table Link' then 'geometry_table_link'
                      when  h.eventInfo.eventAction='store_availability_open_drawer' then 'store_availability_open_drawer'
                      when  h.eventInfo.eventAction='Product Image Zoom' then 'product_image_zoom'
                      when  h.eventInfo.eventAction='Bike leasing more info' then 'bike_leasing_more_info'
                      when  h.eventInfo.eventAction='Hazardous products show more' then 'hazardous_products_show_more'
                      when h.eventInfo.eventCategory='Loqate' then'loqate'
                      when h.eventInfo.eventCategory='Cart/Checkout' then'payback_cart_success'
                  else 'other' end as event_name


                from `bigquery.123456789.ga_sessions_*`,date_range, unnest(hits) as h --206405060
                where _table_suffix between start_date and end_date
                and h.eventInfo. eventCategory is not null),
ga4 as (select
             parse_date('%Y%m%d',event_date) as event_date
            ,event_name
    --event_params.key    
from
    `bigquery.analytics_123456789.events_*`,date_range,unnest(event_params) as event_params  --292798251
where
    _table_suffix between start_date and end_date
group by 1,2)
select eventCategory,eventAction,ga4.event_name as ga4_name, ua.event_name as ua_name
from ua
left outer join ga4 using (event_name)
--where ga4.event_name like ('%succ%')
where eventCategory!='Outbound Links'
group by 1,2,3,4
order by ga4_name asc;


/*
select
             parse_date('%Y%m%d',event_date) as event_date
            ,event_name
            ,(select value.string_value from unnest(event_params) where key = "smartfit_action") as smartfit_action
            --,event_params.key    
from
    `bigquery.analytics_123456789.events_*`--,unnest(event_params) as event_params
where
    _table_suffix between '20231008' and '20231008'
    and event_name='smartfit'
group by 1,2,3;

*/
